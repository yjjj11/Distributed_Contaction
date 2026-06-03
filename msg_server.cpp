#include <grpcpp/grpcpp.h>
#include "msg.grpc.pb.h"
#include "auth.grpc.pb.h"
#include "db.grpc.pb.h"

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/json.hpp>

#include <hiredis/hiredis.h>

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <chrono>
#include <queue>
#include <atomic>
#include <csignal>
#include <cstring>
#include <sstream>
#include <ctime>

namespace json = boost::json;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;
using namespace grpc;

// ========== 配置 ==========
static const int    GRPC_PORT         = 50054;
static const int    WS_PORT           = 50055;
static const char*  REDIS_HOST        = "127.0.0.1";
static const int    REDIS_PORT        = 6379;
static const char*  DB_SERVER_ADDR    = "localhost:50052";
static const char*  AUTH_SERVER_ADDR  = "localhost:50053";
static const int    BATCH_THRESHOLD   = 50;
static const int    BATCH_INTERVAL_MS = 5000;

// ========== 全局状态 ==========

std::mutex g_conns_mutex;
std::unordered_map<int64_t, websocket::stream<tcp::socket>*> g_online_connections;

std::mutex g_batch_mutex;
std::vector<db::MessageRecord> g_batch_queue;
// 消息 ID 改用 Redis INCR msg:id:global，不再使用进程内存计数器

std::atomic<bool> g_running{true};

std::unique_ptr<db::DatabaseService::Stub>   g_db_stub;
std::unique_ptr<auth::AuthService::Stub>     g_auth_stub;

redisContext* g_redis = nullptr;

// ========== Redis 工具 ==========

bool redisConnect() {
  g_redis = redisConnect(REDIS_HOST, REDIS_PORT);
  if (!g_redis || g_redis->err) {
    fprintf(stderr, "[Redis] connect error: %s\n", g_redis ? g_redis->errstr : "unknown");
    return false;
  }
  return true;
}

void redisSetOnline(int64_t user_id) {
  if (!g_redis) return;
  redisCommand(g_redis, "SET online:%ld 1 EX 30", (long)user_id);
}

void redisDelOnline(int64_t user_id) {
  if (!g_redis) return;
  redisCommand(g_redis, "DEL online:%ld", (long)user_id);
}

bool redisIsOnline(int64_t user_id) {
  if (!g_redis) return false;
  redisReply* r = (redisReply*)redisCommand(g_redis, "EXISTS online:%ld", (long)user_id);
  bool ok = (r && r->integer > 0);
  if (r) freeReplyObject(r);
  return ok;
}

void redisPushOfflineMsg(int64_t user_id, const std::string& data) {
  if (!g_redis) return;
  redisCommand(g_redis, "RPUSH offline_msg:%ld %s", (long)user_id, data.c_str());
}

std::vector<std::string> redisPopOfflineMsgs(int64_t user_id) {
  std::vector<std::string> result;
  if (!g_redis) return result;
  redisReply* r = (redisReply*)redisCommand(g_redis, "LRANGE offline_msg:%ld 0 -1", (long)user_id);
  if (r && r->type == REDIS_REPLY_ARRAY) {
    for (size_t i = 0; i < r->elements; ++i)
      if (r->element[i]->str) result.push_back(r->element[i]->str);
  }
  if (r) freeReplyObject(r);
  redisCommand(g_redis, "DEL offline_msg:%ld", (long)user_id);
  return result;
}

void redisPushOfflineFriendReq(int64_t user_id, const std::string& data) {
  if (!g_redis) return;
  redisCommand(g_redis, "RPUSH offline_friend_req:%ld %s", (long)user_id, data.c_str());
}

std::vector<std::string> redisPopOfflineFriendReqs(int64_t user_id) {
  std::vector<std::string> result;
  if (!g_redis) return result;
  redisReply* r = (redisReply*)redisCommand(g_redis, "LRANGE offline_friend_req:%ld 0 -1", (long)user_id);
  if (r && r->type == REDIS_REPLY_ARRAY) {
    for (size_t i = 0; i < r->elements; ++i)
      if (r->element[i]->str) result.push_back(r->element[i]->str);
  }
  if (r) freeReplyObject(r);
  redisCommand(g_redis, "DEL offline_friend_req:%ld", (long)user_id);
  return result;
}

// ========== Redis 消息缓存（ZSET） ==========

static std::string chatCacheKey(int64_t uid1, int64_t uid2) {
  int64_t a = uid1 < uid2 ? uid1 : uid2;
  int64_t b = uid1 < uid2 ? uid2 : uid1;
  return "chat:msgs:" + std::to_string(a) + ":" + std::to_string(b);
}

static void redisCacheMsg(int64_t from_id, int64_t to_id,
                          const std::string& json_data, int64_t ts) {
  if (!g_redis) return;
  auto key = chatCacheKey(from_id, to_id);
  redisCommand(g_redis, "ZADD %s %ld %s", key.c_str(), (long)ts, json_data.c_str());
  redisCommand(g_redis, "EXPIRE %s 7200", key.c_str());  // TTL 2小时
}

static std::vector<std::string> redisGetCachedMsgs(int64_t uid1, int64_t uid2,
                                                    int32_t limit, int32_t offset) {
  std::vector<std::string> result;
  if (!g_redis) return result;
  auto key = chatCacheKey(uid1, uid2);
  // ZREVRANGE 按时间倒序
  redisReply* r = (redisReply*)redisCommand(g_redis,
      "ZREVRANGE %s %d %d", key.c_str(), offset, offset + limit - 1);
  if (r && r->type == REDIS_REPLY_ARRAY) {
    for (size_t i = 0; i < r->elements; ++i)
      if (r->element[i]->str) result.push_back(r->element[i]->str);
  }
  if (r) freeReplyObject(r);
  return result;
}

// ========== Redis 全局 ID & 频控 ==========

static int64_t redisNextMsgId() {
  if (!g_redis) return 0;
  redisReply* r = (redisReply*)redisCommand(g_redis, "INCR msg:id:global");
  if (!r || r->type != REDIS_REPLY_INTEGER) {
    if (r) freeReplyObject(r);
    return 0;
  }
  int64_t id = r->integer;
  freeReplyObject(r);
  return id;
}

static bool redisCheckRate(const std::string& key, int max_count, int window_sec) {
  if (!g_redis) return true;  // Redis 不可用时放行
  redisReply* r = (redisReply*)redisCommand(g_redis, "INCR rate:%s", key.c_str());
  if (!r || r->type != REDIS_REPLY_INTEGER) {
    if (r) freeReplyObject(r);
    return true;
  }
  int count = r->integer;
  freeReplyObject(r);
  if (count == 1) {
    redisCommand(g_redis, "EXPIRE rate:%s %d", key.c_str(), window_sec);
  }
  return count <= max_count;
}

// ========== Auth token 校验 ==========

std::pair<bool, std::pair<int64_t, std::string>> validateToken(const std::string& token) {
  auth::ValidateTokenRequest req;
  req.set_token(token);
  auth::ValidateTokenResponse rsp;
  ClientContext ctx;
  auto status = g_auth_stub->ValidateToken(&ctx, req, &rsp);
  if (status.ok() && rsp.valid()) {
    return std::make_pair(true, std::make_pair(rsp.user_id(), rsp.username()));
  }
  return std::make_pair(false, std::make_pair(0, std::string()));
}

// ========== 工具函数 ==========

std::string currentTimeStr() {
  time_t t = time(nullptr);
  struct tm* tm_info = localtime(&t);
  char buf[64];
  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm_info);
  return buf;
}

std::string currentTimeHMS() {
  time_t t = time(nullptr);
  struct tm* tm_info = localtime(&t);
  char buf[16];
  strftime(buf, sizeof(buf), "%H:%M", tm_info);
  return buf;
}

std::string getUserName(int64_t user_id) {
  db::BatchGetRequest bg;
  bg.add_ids(user_id);
  db::BatchGetResponse bgr;
  ClientContext ctx;
  auto s = g_db_stub->BatchGet(&ctx, bg, &bgr);
  if (s.ok() && bgr.users_size() > 0) {
    return bgr.users(0).username();
  }
  return "user_" + std::to_string(user_id);
}

// 构建 JSON 字符串（避免依赖 nlohmann，用 boost::json 的 O(1) 构造）
std::string make_json_obj(json::object obj) {
  return json::serialize(obj);
}

// ========== gRPC 服务实现 ==========

class MsgServiceImpl final : public msg::MsgService::Service {
public:
  Status RegisterOnline(ServerContext*, const msg::OnlineReq* req,
                        msg::OnlineReply* rsp) override {
    redisSetOnline(req->user_id());
    rsp->set_success(true);
    return Status::OK;
  }

  Status UnregisterOnline(ServerContext*, const msg::OnlineReq* req,
                          msg::OnlineReply* rsp) override {
    redisDelOnline(req->user_id());
    rsp->set_success(true);
    return Status::OK;
  }

  Status SendMessage(ServerContext*, const msg::SendMsgRequest* req,
                     msg::SendMsgReply* rsp) override {
    int64_t from_id = req->from_user_id();
    int64_t to_id = req->to_user_id();
    int64_t now_ts = time(nullptr);
    std::string time_hms = currentTimeHMS();
    int64_t mid = redisNextMsgId();
    std::string from_uname = getUserName(from_id);

    // 加入批处理队列
    {
      db::MessageRecord record;
      record.set_from_user_id(from_id);
      record.set_to_user_id(to_id);
      record.set_content(req->content());
      record.set_msg_type(req->msg_type());
      record.set_status(0);
      std::lock_guard<std::mutex> lk(g_batch_mutex);
      g_batch_queue.push_back(std::move(record));
    }

    // 构造推送消息
    std::string push_str = make_json_obj(json::object{
      {"type", "new_msg"},
      {"from_user_id", from_id},
      {"from_username", from_uname},
      {"content", req->content()},
      {"msg_type", req->msg_type()},
      {"time", time_hms},
      {"created_at", now_ts}
    });

    // 立即缓存到 Redis（历史消息热缓存）
    redisCacheMsg(from_id, to_id, push_str, now_ts);

    bool online = redisIsOnline(to_id);
    if (online) {
      std::lock_guard<std::mutex> lock(g_conns_mutex);
      auto it = g_online_connections.find(to_id);
      if (it != g_online_connections.end() && it->second) {
        beast::error_code ec;
        it->second->write(net::buffer(push_str), ec);
        if (ec) {
          fprintf(stderr, "[WS] push error: %s\n", ec.message().c_str());
        }
      }
    } else {
      redisPushOfflineMsg(to_id, push_str);
    }

    // 给发送者 ack
    std::string ack_str = make_json_obj(json::object{
      {"type", "msg_ack"},
      {"msg_id", mid},
      {"to_user_id", to_id},
      {"created_at", now_ts}
    });
    {
      std::lock_guard<std::mutex> lock(g_conns_mutex);
      auto it = g_online_connections.find(from_id);
      if (it != g_online_connections.end() && it->second) {
        beast::error_code ec;
        it->second->write(net::buffer(ack_str), ec);
      }
    }

    rsp->set_success(true);
    rsp->set_message("ok");
    rsp->set_msg_id(mid);
    rsp->set_created_at(now_ts);
    return Status::OK;
  }

  Status PullOfflineMessages(ServerContext*, const msg::PullOfflineRequest* req,
                              msg::PullOfflineReply* rsp) override {
    auto msgs = redisPopOfflineMsgs(req->user_id());
    for (auto& s : msgs) {
      try {
        auto jv = json::parse(s);
        auto& obj = jv.as_object();
        auto* om = rsp->add_messages();
        if (obj.contains("from_user_id"))
          om->set_from_user_id(obj.at("from_user_id").as_int64());
        if (obj.contains("from_username"))
          om->set_from_username(obj.at("from_username").as_string().c_str());
        if (obj.contains("content"))
          om->set_content(obj.at("content").as_string().c_str());
        if (obj.contains("msg_type"))
          om->set_msg_type((int32_t)obj.at("msg_type").as_int64());
        if (obj.contains("created_at"))
          om->set_created_at(obj.at("created_at").as_int64());
      } catch (...) {}
    }
    rsp->set_message("ok");
    return Status::OK;
  }

  Status AckOfflineMessages(ServerContext*, const msg::AckOfflineRequest*,
                             msg::AckOfflineReply* rsp) override {
    rsp->set_success(true);  // Redis pop 即删除，无需额外操作
    return Status::OK;
  }

  Status SendFriendRequest(ServerContext*, const msg::FriendRequestReq* req,
                            msg::FriendRequestReply* rsp) override {
    int64_t from_id = req->from_user_id();
    std::string from_uname = req->from_username();
    std::string to_uname = req->to_username();

    // 查目标用户
    db::GetUserIdByUsernameReq uq;
    uq.set_username(to_uname);
    db::GetUserIdByUsernameRsp ur;
    ClientContext ctx;
    auto s = g_db_stub->GetUserIdByUsername(&ctx, uq, &ur);
    if (!s.ok() || !ur.found()) {
      rsp->set_success(false);
      rsp->set_message("目标用户不存在");
      return Status::OK;
    }
    int64_t to_id = ur.user_id();

    // 创建请求记录
    db::CreateFriendRequestReq fq;
    fq.set_from_user_id(from_id);
    fq.set_to_user_id(to_id);
    fq.set_from_username(from_uname);
    fq.set_to_username(to_uname);
    db::CreateFriendRequestRsp fr;
    ClientContext ctx2;
    auto cf_status = g_db_stub->CreateFriendRequest(&ctx2, fq, &fr);
    if (!cf_status.ok()) {
      rsp->set_success(false);
      rsp->set_message("创建好友请求失败");
      return Status::OK;
    }

    // 推送好友请求通知
    std::string push_str = make_json_obj(json::object{
      {"type", "new_friend_request"},
      {"request_id", fr.id()},
      {"from_user_id", from_id},
      {"from_username", from_uname},
      {"status", 0}
    });

    bool online = redisIsOnline(to_id);
    if (online) {
      std::lock_guard<std::mutex> lock(g_conns_mutex);
      auto it = g_online_connections.find(to_id);
      if (it != g_online_connections.end() && it->second) {
        beast::error_code ec;
        it->second->write(net::buffer(push_str), ec);
      }
    } else {
      redisPushOfflineFriendReq(to_id, push_str);
    }

    rsp->set_success(true);
    rsp->set_message("好友请求已发送");
    rsp->set_request_id(fr.id());
    return Status::OK;
  }

  Status HandleFriendRequest(ServerContext*, const msg::HandleFriendReq* req,
                              msg::HandleFriendReply* rsp) override {
    int64_t user_id = req->user_id();
    int64_t request_id = req->request_id();
    bool accept = req->accept();

    // 先获取请求详情
    db::GetFriendRequestsByUserReq gq;
    gq.set_user_id(user_id);
    gq.set_status(-1);
    db::GetFriendRequestsByUserRsp gr;
    ClientContext ctx0;
    auto gs = g_db_stub->GetFriendRequestsByUser(&ctx0, gq, &gr);
    if (!gs.ok()) {
      rsp->set_success(false);
      rsp->set_message("查询请求失败");
      return Status::OK;
    }
    int64_t friend_id = 0;
    std::string friend_uname;
    for (auto& r : gr.requests()) {
      if (r.id() == request_id) {
        friend_id = r.from_user_id();
        friend_uname = r.from_username();
        break;
      }
    }

    // 更新请求状态
    db::UpdateFriendRequestStatusReq us;
    us.set_id(request_id);
    us.set_status(accept ? 1 : 2);
    db::UpdateFriendRequestStatusRsp ur;
    ClientContext ctx;
        auto us_status = g_db_stub->UpdateFriendRequestStatus(&ctx, us, &ur);

    if (accept && friend_id > 0) {
      std::string my_uname = getUserName(user_id);
      // 双向好友关系
      {
        db::CreateFriendshipReq cf;
        cf.set_user_id(user_id);
        cf.set_friend_id(friend_id);
        cf.set_friend_username(friend_uname);
        ClientContext ctx3;
        db::CreateFriendshipRsp cfr;
        g_db_stub->CreateFriendship(&ctx3, cf, &cfr);
      }
      {
        db::CreateFriendshipReq cf;
        cf.set_user_id(friend_id);
        cf.set_friend_id(user_id);
        cf.set_friend_username(my_uname);
        ClientContext ctx4;
        db::CreateFriendshipRsp cfr;
        g_db_stub->CreateFriendship(&ctx4, cf, &cfr);
      }

      // 通知请求发起方
      std::string notify = make_json_obj(json::object{
        {"type", "friend_request_accepted"},
        {"friend_id", user_id},
        {"friend_username", my_uname}
      });
      {
        std::lock_guard<std::mutex> lock(g_conns_mutex);
        auto it = g_online_connections.find(friend_id);
        if (it != g_online_connections.end() && it->second) {
          beast::error_code ec;
          it->second->write(net::buffer(notify), ec);
        }
      }
    }

    rsp->set_success(true);
    rsp->set_message(accept ? "已接受" : "已拒绝");
    return Status::OK;
  }

  Status GetPendingFriendRequests(ServerContext*, const msg::GetPendingReqsReq* req,
                                   msg::GetPendingReqsReply* rsp) override {
    db::GetFriendRequestsByUserReq gq;
    gq.set_user_id(req->user_id());
    gq.set_status(0);
    db::GetFriendRequestsByUserRsp gr;
    ClientContext ctx;
    g_db_stub->GetFriendRequestsByUser(&ctx, gq, &gr);
    for (auto& r : gr.requests()) {
      auto* p = rsp->add_requests();
      p->set_request_id(r.id());
      p->set_from_user_id(r.from_user_id());
      p->set_from_username(r.from_username());
      p->set_status(r.status());
      p->set_created_at(r.created_at());
    }
    rsp->set_message("ok");
    return Status::OK;
  }

  Status GetFriendList(ServerContext*, const msg::GetFriendListReq* req,
                        msg::GetFriendListReply* rsp) override {
    db::GetFriendshipsReq gf;
    gf.set_user_id(req->user_id());
    db::GetFriendshipsRsp gfr;
    ClientContext ctx;
    g_db_stub->GetFriendships(&ctx, gf, &gfr);
    for (auto& f : gfr.friendships()) {
      auto* fi = rsp->add_friends();
      fi->set_friend_id(f.friend_id());
      fi->set_friend_username(f.friend_username());
      fi->set_online(redisIsOnline(f.friend_id()));
      fi->set_created_at(f.created_at());
    }
    rsp->set_message("ok");
    return Status::OK;
  }

  Status GetHistoryMessages(ServerContext*, const msg::GetHistoryReq* req,
                             msg::GetHistoryReply* rsp) override {
    int32_t limit = req->limit() > 0 ? req->limit() : 50;
    int32_t offset = req->offset();

    // 1. 先查 Redis 热缓存
    auto cached = redisGetCachedMsgs(req->user_id(), req->with_user_id(), limit, offset);
    if (!cached.empty()) {
      for (auto& s : cached) {
        try {
          auto jv = json::parse(s);
          auto& obj = jv.as_object();
          auto* hm = rsp->add_messages();
          if (obj.contains("from_user_id"))
            hm->set_from_user_id(obj.at("from_user_id").as_int64());
          if (obj.contains("content"))
            hm->set_content(obj.at("content").as_string().c_str());
          if (obj.contains("msg_type"))
            hm->set_msg_type((int32_t)obj.at("msg_type").as_int64());
          if (obj.contains("created_at"))
            hm->set_created_at(std::to_string((int64_t)obj.at("created_at").as_int64()));
        } catch (...) {}
      }
      rsp->set_message("ok");
      return Status::OK;
    }

    // 2. Redis 无缓存，回退到 DB
    db::GetMessagesBetweenReq gm;
    gm.set_user_id_a(req->user_id());
    gm.set_user_id_b(req->with_user_id());
    gm.set_limit(limit);
    gm.set_offset(offset);
    db::GetMessagesBetweenRsp gmr;
    ClientContext ctx;
    g_db_stub->GetMessagesBetween(&ctx, gm, &gmr);
    for (auto& m : gmr.messages()) {
      auto* hm = rsp->add_messages();
      hm->set_msg_id(m.id());
      hm->set_from_user_id(m.from_user_id());
      hm->set_content(m.content());
      hm->set_msg_type(m.msg_type());
      hm->set_created_at(m.created_at());
    }
    rsp->set_message("ok");
    return Status::OK;
  }

  Status GetUserByToken(ServerContext*, const msg::GetUserByTokenReq* req,
                         msg::GetUserByTokenReply* rsp) override {
    auto [valid, info] = validateToken(req->token());
    rsp->set_valid(valid);
    if (valid) {
      rsp->set_user_id(info.first);
      rsp->set_username(info.second);
    }
    return Status::OK;
  }
};

// ========== WebSocket 会话处理 ==========

void ws_session(websocket::stream<tcp::socket> ws, tcp::endpoint ep) {
  beast::error_code ec;

  // 1. Accept WebSocket upgrade
  ws.accept(ec);
  if (ec) {
    fprintf(stderr, "[WS] accept error: %s\n", ec.message().c_str());
    return;
  }

  // 2. 解析 token
  std::string token;
  // 从 websocket stream 的底层 socket 拿不到 query，但我们可以在 accept 时从 request 拿
  // 实际上 Boost.Beast 的 accept() 接受 request, 这里简化：用 hardcoded 方式
  // 更简单的方式：用户连接时发第一条消息做 auth
  // 但我们设计用 URL query param, 需要换一种方式
  
  // 实际上 Boost.Beast websocket stream 的 accept 默认从请求中提取信息
  // 我们改用 accept(请求) 的方式
  
  // 简化方案：连接后客户端先发 auth 消息
  // 实际上更好的方式：我们在 ws_listener 中拿到 HTTP 请求再升级
  // 但为了简单，这次我们用"第一条消息鉴权"模式
  
  // 先发个欢迎消息让客户端知道需要鉴权
  std::string greet = make_json_obj(json::object{
    {"type", "need_auth"},
    {"message", "请发送 {\"action\":\"auth\",\"token\":\"...\"}"}
  });
  ws.write(net::buffer(greet), ec);

  int64_t user_id = 0;
  std::string username;
  bool authenticated = false;

  // 3. 消息循环
  beast::flat_buffer buffer;
  while (g_running && !authenticated) {
    buffer.clear();
    ec = {};
    ws.read(buffer, ec);
    if (ec) break;

    std::string data = beast::buffers_to_string(buffer.data());
    try {
      auto jv = json::parse(data);
      auto& obj = jv.as_object();
      std::string action = obj.at("action").as_string().c_str();

      if (action == "auth") {
        std::string token_val = obj.at("token").as_string().c_str();
        auto [valid, info] = validateToken(token_val);
        if (valid) {
          user_id = info.first;
          username = info.second;
          authenticated = true;

          redisSetOnline(user_id);
          {
            std::lock_guard<std::mutex> lock(g_conns_mutex);
            g_online_connections[user_id] = &ws;
          }

          printf("[WS] user %ld (%s) authenticated\n", (long)user_id, username.c_str());

          // 发送鉴权成功
          std::string ok = make_json_obj(json::object{
            {"type", "auth_ok"},
            {"user_id", user_id},
            {"username", username}
          });
          ws.write(net::buffer(ok), ec);

          // 推送离线消息
          auto offline_msgs = redisPopOfflineMsgs(user_id);
          if (!offline_msgs.empty()) {
            json::array arr;
            for (auto& m : offline_msgs) {
              try { arr.push_back(json::parse(m)); } catch (...) {}
            }
            json::object batch;
            batch["type"] = "offline_msgs";
            batch["messages"] = arr;
            std::string s = json::serialize(batch);
            ws.write(net::buffer(s), ec);
          }

          // 推送离线好友请求
          auto offline_reqs = redisPopOfflineFriendReqs(user_id);
          for (auto& r : offline_reqs) {
            ws.write(net::buffer(r), ec);
          }
        } else {
          std::string err = make_json_obj(json::object{
            {"type", "auth_error"},
            {"message", "token invalid"}
          });
          ws.write(net::buffer(err), ec);
        }
      } else {
        std::string err = make_json_obj(json::object{
          {"type", "error"},
          {"message", "请先发送 auth"}
        });
        ws.write(net::buffer(err), ec);
      }
    } catch (...) {
      std::string err = make_json_obj(json::object{
        {"type", "error"},
        {"message", "invalid json"}
      });
      ws.write(net::buffer(err), ec);
    }
  }

  if (!authenticated) {
    // 鉴权失败
    try { ws.close(websocket::close_code::normal, ec); } catch (...) {}
    return;
  }

  // 4. 正常消息循环
  while (g_running) {
    buffer.clear();
    ec = {};
    ws.read(buffer, ec);

    if (ec == websocket::error::closed ) break;
    if (ec) {
      fprintf(stderr, "[WS] read error: %s\n", ec.message().c_str());
      break;
    }

    std::string data = beast::buffers_to_string(buffer.data());
    try {
      auto jv = json::parse(data);
      auto& obj = jv.as_object();
      std::string action = obj.at("action").as_string().c_str();

      if (action == "ping") {
        redisSetOnline(user_id);
        std::string pong = make_json_obj(json::object{{"type", "pong"}});
        ws.write(net::buffer(pong), ec);
      }
      else if (action == "send_msg") {
        int64_t to_user_id = obj.at("to_user_id").as_int64();
        std::string content = obj.at("content").as_string().c_str();
        int64_t now_ts = time(nullptr);
        std::string time_hms = currentTimeHMS();

        // 频控：每分钟最多 30 条
        if (!redisCheckRate("msg:" + std::to_string(user_id), 30, 60)) {
          std::string err = make_json_obj(json::object{
            {"type", "error"}, {"message", "rate limited: max 30 msg/min"}
          });
          ws.write(net::buffer(err), ec);
          continue;
        }
        int64_t mid = redisNextMsgId();

        // 批处理队列
        {
          db::MessageRecord record;
          record.set_from_user_id(user_id);
          record.set_to_user_id(to_user_id);
          record.set_content(content);
          record.set_msg_type(0);
          record.set_status(0);
          std::lock_guard<std::mutex> lk(g_batch_mutex);
          g_batch_queue.push_back(std::move(record));
        }

        // 构造推送
        std::string push_str = make_json_obj(json::object{
          {"type", "new_msg"},
          {"from_user_id", user_id},
          {"from_username", username},
          {"content", content},
          {"msg_type", 0},
          {"time", time_hms},
          {"created_at", now_ts}
        });

        // 立即缓存到 Redis（历史消息热缓存）
        redisCacheMsg(user_id, to_user_id, push_str, now_ts);

        bool online = redisIsOnline(to_user_id);
        if (online) {
          std::lock_guard<std::mutex> lock(g_conns_mutex);
          auto it = g_online_connections.find(to_user_id);
          if (it != g_online_connections.end() && it->second) {
            beast::error_code wec;
            it->second->write(net::buffer(push_str), wec);
          }
        } else {
          redisPushOfflineMsg(to_user_id, push_str);
        }

        // ack to sender
        std::string ack = make_json_obj(json::object{
          {"type", "msg_ack"},
          {"msg_id", mid},
          {"to_user_id", to_user_id},
          {"created_at", now_ts}
        });
        ws.write(net::buffer(ack), ec);
      }
      else if (action == "send_friend_request") {
        // 频控：每小时最多 20 次
        if (!redisCheckRate("friend_req:" + std::to_string(user_id), 20, 3600)) {
          std::string err = make_json_obj(json::object{
            {"type", "error"}, {"message", "rate limited: max 20 friend requests/hour"}
          });
          ws.write(net::buffer(err), ec);
          continue;
        }
        std::string to_uname = obj.at("to_username").as_string().c_str();

        db::GetUserIdByUsernameReq uq;
        uq.set_username(to_uname);
        db::GetUserIdByUsernameRsp ur;
        ClientContext ctx;
        auto s = g_db_stub->GetUserIdByUsername(&ctx, uq, &ur);
        if (!s.ok() || !ur.found()) {
          std::string err = make_json_obj(json::object{
            {"type", "error"}, {"message", "目标用户不存在"}
          });
          ws.write(net::buffer(err), ec);
          continue;
        }
        int64_t to_id = ur.user_id();

        db::CreateFriendRequestReq fq;
        fq.set_from_user_id(user_id);
        fq.set_to_user_id(to_id);
        fq.set_from_username(username);
        fq.set_to_username(to_uname);
        db::CreateFriendRequestRsp fr;
        ClientContext ctx2;
        g_db_stub->CreateFriendRequest(&ctx2, fq, &fr);

        std::string push = make_json_obj(json::object{
          {"type", "new_friend_request"},
          {"request_id", fr.id()},
          {"from_user_id", user_id},
          {"from_username", username},
          {"status", 0}
        });

        bool target_online = redisIsOnline(to_id);
        if (target_online) {
          std::lock_guard<std::mutex> lock(g_conns_mutex);
          auto it = g_online_connections.find(to_id);
          if (it != g_online_connections.end() && it->second) {
            beast::error_code wec;
            it->second->write(net::buffer(push), wec);
          }
        } else {
          redisPushOfflineFriendReq(to_id, push);
        }

        std::string ack = make_json_obj(json::object{
          {"type", "friend_request_ack"},
          {"success", true},
          {"request_id", fr.id()}
        });
        ws.write(net::buffer(ack), ec);
      }
      else if (action == "handle_friend_request") {
        int64_t request_id = obj.at("request_id").as_int64();
        bool accept = obj.at("accept").as_bool();

        // 获取请求详情
        db::GetFriendRequestsByUserReq gq;
        gq.set_user_id(user_id);
        gq.set_status(-1);
        db::GetFriendRequestsByUserRsp gr;
        ClientContext ctx0;
        auto gs = g_db_stub->GetFriendRequestsByUser(&ctx0, gq, &gr);
        if (!gs.ok()) {
          std::string err = make_json_obj(json::object{
            {"type", "error"}, {"message", "查询请求失败"}
          });
          ws.write(net::buffer(err), ec);
          continue;
        }
        int64_t friend_id = 0;
        std::string friend_uname;
        for (auto& r : gr.requests()) {
          if (r.id() == request_id) {
            friend_id = r.from_user_id();
            friend_uname = r.from_username();
            break;
          }
        }

        db::UpdateFriendRequestStatusReq us;
        us.set_id(request_id);
        us.set_status(accept ? 1 : 2);
        db::UpdateFriendRequestStatusRsp ur;
        ClientContext ctx;
        g_db_stub->UpdateFriendRequestStatus(&ctx, us, &ur);

        if (accept && friend_id > 0) {
          // 双向好友
          {
            db::CreateFriendshipReq cf;
            cf.set_user_id(user_id);
            cf.set_friend_id(friend_id);
            cf.set_friend_username(friend_uname);
            ClientContext ctx3;
            db::CreateFriendshipRsp cfr;
            g_db_stub->CreateFriendship(&ctx3, cf, &cfr);
          }
          {
            db::CreateFriendshipReq cf;
            cf.set_user_id(friend_id);
            cf.set_friend_id(user_id);
            cf.set_friend_username(username);
            ClientContext ctx4;
            db::CreateFriendshipRsp cfr;
            g_db_stub->CreateFriendship(&ctx4, cf, &cfr);
          }

          // 通知发起方
          std::string notify = make_json_obj(json::object{
            {"type", "friend_request_accepted"},
            {"friend_id", user_id},
            {"friend_username", username}
          });
          {
            std::lock_guard<std::mutex> lock(g_conns_mutex);
            auto it = g_online_connections.find(friend_id);
            if (it != g_online_connections.end() && it->second) {
              beast::error_code wec;
              it->second->write(net::buffer(notify), wec);
            }
          }
        }

        std::string ack = make_json_obj(json::object{
          {"type", "handle_friend_ack"},
          {"success", true},
          {"message", accept ? "已接受" : "已拒绝"},
          {"request_id", request_id}
        });
        ws.write(net::buffer(ack), ec);
      }
      else if (action == "typing") {
        int64_t to_uid = obj.at("to_user_id").as_int64();
        // Redis: typing 状态 10 秒后自动过期
        redisCommand(g_redis, "SET typing:%ld:%ld 1 EX 10", (long)user_id, (long)to_uid);
        // 推送给接收方
        bool to_online = redisIsOnline(to_uid);
        if (to_online) {
          std::lock_guard<std::mutex> lock(g_conns_mutex);
          auto it = g_online_connections.find(to_uid);
          if (it != g_online_connections.end() && it->second) {
            std::string typing_notify = make_json_obj(json::object{
              {"type", "typing"},
              {"from_user_id", user_id},
              {"from_username", username}
            });
            beast::error_code wec;
            it->second->write(net::buffer(typing_notify), wec);
          }
        }
      }
      else {
        std::string err = make_json_obj(json::object{
          {"type", "error"}, {"message", "unknown action: " + action}
        });
        ws.write(net::buffer(err), ec);
      }
    } catch (std::exception& e) {
      fprintf(stderr, "[WS] error: %s\n", e.what());
      std::string err = make_json_obj(json::object{
        {"type", "error"}, {"message", "invalid json"}
      });
      ws.write(net::buffer(err), ec);
    }
  }

  // 5. 清理
  {
    std::lock_guard<std::mutex> lock(g_conns_mutex);
    g_online_connections.erase(user_id);
  }
  redisDelOnline(user_id);
  printf("[WS] user %ld disconnected\n", (long)user_id);

  try { ws.close(websocket::close_code::normal, ec); } catch (...) {}
}

// ========== WebSocket 监听 ==========

void ws_listen() {
  try {
    net::io_context ioc{1};
    tcp::acceptor acceptor{ioc, tcp::endpoint{tcp::v4(), WS_PORT}};
    printf("[WS] WebSocket listening on 0.0.0.0:%d\n", WS_PORT);

    while (g_running) {
      tcp::socket socket{ioc};
      acceptor.accept(socket);
      std::thread(ws_session,
                  websocket::stream<tcp::socket>(std::move(socket)),
                  socket.remote_endpoint())
          .detach();
    }
  } catch (std::exception& e) {
    fprintf(stderr, "[WS] listener error: %s\n", e.what());
  }
}

// ========== 批处理持久化线程 ==========

void batch_persist_thread() {
  printf("[Batch] persist thread started (threshold=%d, interval=%dms)\n",
         BATCH_THRESHOLD, BATCH_INTERVAL_MS);
  while (g_running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(BATCH_INTERVAL_MS));
    std::vector<db::MessageRecord> batch;
    {
      std::lock_guard<std::mutex> lock(g_batch_mutex);
      if ((int)g_batch_queue.size() < BATCH_THRESHOLD) continue;
      batch.swap(g_batch_queue);
    }
    if (!batch.empty()) {
      db::BatchCreateMessagesReq req;
      for (auto& m : batch)
        *req.add_messages() = std::move(m);
      db::BatchCreateMessagesRsp rsp;
      ClientContext ctx;
      auto s = g_db_stub->BatchCreateMessages(&ctx, req, &rsp);
      if (s.ok()) {
        printf("[Batch] persisted %zu messages\n", batch.size());
      } else {
        fprintf(stderr, "[Batch] persist error: %s\n", s.error_message().c_str());
        std::lock_guard<std::mutex> lock(g_batch_mutex);
        for (auto& m : batch)
          g_batch_queue.push_back(std::move(m));
      }
    }
  }
}

// ========== gRPC 服务器 ==========

void run_grpc_server() {
  std::string addr = "0.0.0.0:" + std::to_string(GRPC_PORT);
  MsgServiceImpl svc;
  ServerBuilder builder;
  builder.AddListeningPort(addr, InsecureServerCredentials());
  builder.RegisterService(&svc);
  auto server = builder.BuildAndStart();
  printf("[gRPC] MsgService listening on %s\n", addr.c_str());
  server->Wait();
}

// ========== main ==========

int main() {
  // 连接 gRPC stubs
  {
    auto channel = grpc::CreateChannel(DB_SERVER_ADDR, InsecureChannelCredentials());
    g_db_stub = db::DatabaseService::NewStub(channel);
  }
  {
    auto channel = grpc::CreateChannel(AUTH_SERVER_ADDR, InsecureChannelCredentials());
    g_auth_stub = auth::AuthService::NewStub(channel);
  }

  if (!redisConnect()) {
    fprintf(stderr, "Redis connection failed\n");
    return 1;
  }

  printf("=== msgServer starting ===\n");
  printf("  gRPC : %d, WS : %d\n", GRPC_PORT, WS_PORT);

  std::thread grpc_thread(run_grpc_server);
  std::thread ws_thread(ws_listen);
  std::thread batch_thread(batch_persist_thread);

  grpc_thread.join();
  ws_thread.join();
  batch_thread.join();
  return 0;
}
