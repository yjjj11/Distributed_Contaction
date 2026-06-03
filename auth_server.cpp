#include <grpcpp/grpcpp.h>
#include "auth.grpc.pb.h"
#include <openssl/hmac.h>
#include <hiredis/hiredis.h>
#include <string>
#include <cstring>
#include <ctime>
#include <thread>
#include <chrono>

using namespace grpc;

// ---------- JWT 工具函数 ----------

static std::string base64url_encode(const unsigned char* data, size_t len) {
  static const char b64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  out.reserve(((len + 2) / 3) * 4);
  for (size_t i = 0; i < len; i += 3) {
    unsigned char b0 = data[i];
    unsigned char b1 = i + 1 < len ? data[i + 1] : 0;
    unsigned char b2 = i + 2 < len ? data[i + 2] : 0;
    out.push_back(b64[b0 >> 2]);
    out.push_back(b64[((b0 & 0x03) << 4) | (b1 >> 4)]);
    out.push_back(i + 1 < len ? b64[((b1 & 0x0f) << 2) | (b2 >> 6)] : '=');
    out.push_back(i + 2 < len ? b64[b2 & 0x3f] : '=');
  }
  for (auto& ch : out) {
    if (ch == '+') ch = '-';
    else if (ch == '/') ch = '_';
  }
  auto pos = out.find_last_not_of('=');
  if (pos != std::string::npos) out.resize(pos + 1);
  return out;
}

static std::string base64url_decode(const std::string& in) {
  std::string tmp = in;
  for (auto& ch : tmp) {
    if (ch == '-') ch = '+';
    else if (ch == '_') ch = '/';
  }
  size_t pad = (4 - tmp.size() % 4) % 4;
  tmp.append(pad, '=');

  auto pos = [](char c) -> unsigned char {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return 0;
  };

  std::string out;
  out.reserve((tmp.size() / 4) * 3);
  for (size_t i = 0; i < tmp.size(); i += 4) {
    unsigned char b0 = pos(tmp[i]);
    unsigned char b1 = pos(tmp[i + 1]);
    unsigned char b2 = pos(tmp[i + 2]);
    unsigned char b3 = pos(tmp[i + 3]);
    out.push_back((b0 << 2) | (b1 >> 4));
    if (tmp[i + 2] != '=') out.push_back((b1 << 4) | (b2 >> 2));
    if (tmp[i + 3] != '=') out.push_back((b2 << 6) | b3);
  }
  return out;
}

static std::string hmac_sha256(const std::string& key, const std::string& data) {
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int md_len;
  HMAC(EVP_sha256(), key.data(), key.size(),
       reinterpret_cast<const unsigned char*>(data.data()), data.size(),
       md, &md_len);
  return std::string(reinterpret_cast<char*>(md), md_len);
}

static std::string create_jwt(int64_t user_id, const std::string& username,
                               const std::string& secret) {
  // header
  std::string header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
  std::string hdr_b64 = base64url_encode(
      reinterpret_cast<const unsigned char*>(header.data()), header.size());

  // payload: {"user_id":...,"username":"...","iat":...,"exp":...}
  auto now = std::time(nullptr);
  auto exp = now + 86400;  // 24h
  std::string payload =
      "{\"user_id\":" + std::to_string(user_id) +
      ",\"username\":\"" + username + "\",\"iat\":" + std::to_string(now) +
      ",\"exp\":" + std::to_string(exp) + "}";
  std::string pay_b64 = base64url_encode(
      reinterpret_cast<const unsigned char*>(payload.data()), payload.size());

  std::string signing_input = hdr_b64 + "." + pay_b64;
  std::string sig = hmac_sha256(secret, signing_input);
  std::string sig_b64 = base64url_encode(
      reinterpret_cast<const unsigned char*>(sig.data()), sig.size());

  return signing_input + "." + sig_b64;
}

static bool verify_jwt(const std::string& token, const std::string& secret,
                       int64_t& user_id, std::string& username,
                       int64_t& expiration, int64_t& iat) {
  auto pos1 = token.find('.');
  if (pos1 == std::string::npos) return false;
  auto pos2 = token.find('.', pos1 + 1);
  if (pos2 == std::string::npos) return false;

  std::string hdr_b64 = token.substr(0, pos1);
  std::string pay_b64 = token.substr(pos1 + 1, pos2 - pos1 - 1);
  std::string sig_b64 = token.substr(pos2 + 1);

  // verify signature
  std::string signing_input = hdr_b64 + "." + pay_b64;
  std::string expected_sig = hmac_sha256(secret, signing_input);
  std::string expected_sig_b64 = base64url_encode(
      reinterpret_cast<const unsigned char*>(expected_sig.data()), expected_sig.size());

  if (sig_b64 != expected_sig_b64) return false;

  // decode payload
  std::string payload_json = base64url_decode(pay_b64);
  // parse json by hand (minimal JSON parser)
  auto find_int = [&](const std::string& key) -> int64_t {
    auto kpos = payload_json.find("\"" + key + "\"");
    if (kpos == std::string::npos) return 0;
    auto cpos = payload_json.find(':', kpos);
    if (cpos == std::string::npos) return 0;
    cpos++;
    while (cpos < payload_json.size() && payload_json[cpos] == ' ') cpos++;
    int64_t val = 0;
    bool neg = false;
    if (cpos < payload_json.size() && payload_json[cpos] == '-') { neg = true; cpos++; }
    while (cpos < payload_json.size() && payload_json[cpos] >= '0' && payload_json[cpos] <= '9') {
      val = val * 10 + (payload_json[cpos] - '0');
      cpos++;
    }
    return neg ? -val : val;
  };
  auto find_str = [&](const std::string& key) -> std::string {
    auto kpos = payload_json.find("\"" + key + "\"");
    if (kpos == std::string::npos) return "";
    auto cpos = payload_json.find(':', kpos);
    if (cpos == std::string::npos) return "";
    cpos++;
    while (cpos < payload_json.size() && payload_json[cpos] == ' ') cpos++;
    if (cpos >= payload_json.size() || payload_json[cpos] != '"') return "";
    cpos++;
    std::string val;
    while (cpos < payload_json.size() && payload_json[cpos] != '"') {
      if (payload_json[cpos] == '\\' && cpos + 1 < payload_json.size()) {
        cpos++;
        if (payload_json[cpos] == 'n') val += '\n';
        else if (payload_json[cpos] == 't') val += '\t';
        else if (payload_json[cpos] == '\\') val += '\\';
        else if (payload_json[cpos] == '"') val += '"';
        else { val += '\\'; val += payload_json[cpos]; }
      } else {
        val += payload_json[cpos];
      }
      cpos++;
    }
    return val;
  };

  user_id = find_int("user_id");
  username = find_str("username");
  expiration = find_int("exp");
  iat = find_int("iat");

  auto now = std::time(nullptr);
  if (now > expiration) return false;

  return true;
}

// ---------- Redis 工具 ----------

static redisContext* g_redis = nullptr;

static bool init_redis(const std::string& host = "127.0.0.1", int port = 6379) {
  g_redis = redisConnect(host.c_str(), port);
  if (!g_redis || g_redis->err) {
    if (g_redis) {
      fprintf(stderr, "Redis connection error: %s\n", g_redis->errstr);
      redisFree(g_redis);
      g_redis = nullptr;
    }
    return false;
  }
  return true;
}

static bool is_token_revoked(int64_t user_id, int64_t iat) {
  if (!g_redis) return false;
  std::string key = "revoked:user:" + std::to_string(user_id);
  redisReply* reply = (redisReply*)redisCommand(g_redis, "GET %s", key.c_str());
  if (!reply) return false;
  bool revoked = false;
  if (reply->type == REDIS_REPLY_STRING && reply->str) {
    // 只有当 token 签发时间 早于或等于 吊销时间 时才算吊销
    int64_t revoke_ts = std::atoll(reply->str);
    revoked = (iat <= revoke_ts);
  }
  freeReplyObject(reply);
  return revoked;
}

static bool revoke_token(int64_t user_id, int64_t ttl_seconds) {
  if (!g_redis) return false;
  std::string key = "revoked:user:" + std::to_string(user_id);
  int64_t now = std::time(nullptr);
  redisReply* reply = (redisReply*)redisCommand(g_redis, "SET %s %ld EX %ld",
                                                  key.c_str(), (long)now, ttl_seconds);
  if (!reply) return false;
  bool ok = (reply->type == REDIS_REPLY_STATUS &&
             strcasecmp(reply->str, "OK") == 0);
  freeReplyObject(reply);
  return ok;
}

// ---------- 频控 ----------

static bool redisCheckRate(const std::string& key, int max_count, int window_sec) {
  if (!g_redis) return true;
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

// ---------- AuthServiceImpl ----------

class AuthServiceImpl final : public auth::AuthService::Service {
  std::string secret_;

 public:
  AuthServiceImpl(const std::string& secret) : secret_(secret) {}

  Status CreateToken(ServerContext*, const auth::CreateTokenRequest* req,
                     auth::CreateTokenResponse* rsp) override {
    // 频控：每分钟最多 10 次登录/Token 创建（防暴力破解）
    if (!redisCheckRate("login:" + req->username(), 10, 60)) {
      rsp->set_success(false);
      rsp->set_message("rate limited: max 10 login attempts/min");
      return Status::OK;
    }
    std::string token = create_jwt(req->user_id(), req->username(), secret_);
    rsp->set_success(true);
    rsp->set_token(token);
    rsp->set_message("token created");
    return Status::OK;
  }

  Status ValidateToken(ServerContext*, const auth::ValidateTokenRequest* req,
                       auth::ValidateTokenResponse* rsp) override {
    int64_t user_id = 0;
    std::string username;
    int64_t exp = 0;
    int64_t iat = 0;
    if (!verify_jwt(req->token(), secret_, user_id, username, exp, iat)) {
      rsp->set_valid(false);
      rsp->set_message("invalid or expired token");
      return Status::OK;
    }

    // 检查 Redis 黑名单（基于签发时间，新 token 不受旧吊销影响）
    if (is_token_revoked(user_id, iat)) {
      rsp->set_valid(false);
      rsp->set_message("token revoked (logged out)");
      return Status::OK;
    }

    rsp->set_valid(true);
    rsp->set_user_id(user_id);
    rsp->set_username(username);
    rsp->set_exp(exp);
    rsp->set_message("token valid");
    return Status::OK;
  }

  Status RevokeToken(ServerContext*, const auth::RevokeTokenRequest* req,
                     auth::RevokeTokenResponse* rsp) override {
    if (!g_redis) {
      rsp->set_success(false);
      rsp->set_message("redis not available");
      return Status::OK;
    }
    bool ok = revoke_token(req->user_id(), req->ttl_seconds());
    rsp->set_success(ok);
    rsp->set_message(ok ? "token revoked" : "revoke failed");
    return Status::OK;
  }
};

void RunServer(const std::string& addr, const std::string& secret) {
  // 尝试连接 Redis（非致命，黑名单功能降级）
  if (!init_redis()) {
    fprintf(stderr, "Warning: Redis not available, token revocation disabled\n");
  }

  AuthServiceImpl service(secret);

  ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  if (!server) {
    std::cerr << "Failed to start auth server on " << addr << std::endl;
    return;
  }
  std::cout << "Auth server listening on " << addr << std::endl;
  server->Wait();
}

int main(int argc, char** argv) {
  std::string addr = "0.0.0.0:50053";
  std::string secret = "auth-secret-key-change-me";
  for (int i = 1; i + 1 < argc; i += 2) {
    std::string key = argv[i];
    if (key == "--addr" || key == "-a") addr = argv[i + 1];
    else if (key == "--port" || key == "-p") addr = "0.0.0.0:" + std::string(argv[i + 1]);
    else if (key == "--secret" || key == "-s") secret = argv[i + 1];
  }
  RunServer(addr, secret);
  return 0;
}
