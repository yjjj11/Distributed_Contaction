#include <grpcpp/grpcpp.h>
#include "db.grpc.pb.h"
#include "db_proxy.hpp"

using namespace grpc;
using namespace db;

class DBServiceImpl final : public DatabaseService::Service {
  DBProxy* proxy_;

public:
  explicit DBServiceImpl(DBProxy* p) : proxy_(p) {}

  Status BatchCreate(ServerContext*, const BatchCreateRequest* req,
                     BatchCreateResponse* rsp) override {
    auto users = std::vector<db::User>(req->users().begin(), req->users().end());
    int64_t n = proxy_->batchCreate(users);
    if (n >= 0) {
      rsp->set_affected_count(static_cast<int32_t>(n));
      rsp->set_message("ok");
    } else {
      rsp->set_affected_count(0);
      rsp->set_message("batch create failed");
    }
    return Status::OK;
  }

  Status BatchGet(ServerContext*, const BatchGetRequest* req,
                  BatchGetResponse* rsp) override {
    auto ids = std::vector<int64_t>(req->ids().begin(), req->ids().end());
    auto users = proxy_->batchGet(ids);
    for (auto& u : users)
      *rsp->add_users() = std::move(u);
    rsp->set_message("ok");
    return Status::OK;
  }

  Status BatchUpdate(ServerContext*, const BatchUpdateRequest* req,
                     BatchUpdateResponse* rsp) override {
    auto users = std::vector<db::User>(req->users().begin(), req->users().end());
    int64_t n = proxy_->batchUpdate(users);
    rsp->set_affected_count(static_cast<int32_t>(n >= 0 ? n : 0));
    rsp->set_message(n >= 0 ? "ok" : "batch update failed");
    return Status::OK;
  }

  Status BatchDelete(ServerContext*, const BatchDeleteRequest* req,
                     BatchDeleteResponse* rsp) override {
    auto ids = std::vector<int64_t>(req->ids().begin(), req->ids().end());
    int64_t n = proxy_->batchDelete(ids);
    rsp->set_affected_count(static_cast<int32_t>(n >= 0 ? n : 0));
    rsp->set_message(n >= 0 ? "ok" : "batch delete failed");
    return Status::OK;
  }

  Status Query(ServerContext*, const QueryRequest* req,
               QueryResponse* rsp) override {
    auto [users, total] = proxy_->query(req->field(), req->value(),
                                        req->page(), req->page_size());
    for (auto& u : users)
      *rsp->add_users() = std::move(u);
    rsp->set_total(static_cast<int32_t>(total));
    rsp->set_message("ok");
    return Status::OK;
  }

  // ========== 好友请求 ==========

  Status CreateFriendRequest(ServerContext*,
                              const CreateFriendRequestReq* req,
                              CreateFriendRequestRsp* rsp) override {
    int64_t id = proxy_->createFriendRequest(
        req->from_user_id(), req->to_user_id(),
        req->from_username(), req->to_username());
    if (id >= 0) {
      rsp->set_id(id);
      rsp->set_message("ok");
    } else {
      rsp->set_id(0);
      rsp->set_message("create friend request failed");
    }
    return Status::OK;
  }

  Status GetFriendRequestsByUser(ServerContext*,
                                  const GetFriendRequestsByUserReq* req,
                                  GetFriendRequestsByUserRsp* rsp) override {
    auto requests = proxy_->getFriendRequestsByUser(req->user_id(), req->status());
    for (auto& r : requests)
      *rsp->add_requests() = std::move(r);
    rsp->set_message("ok");
    return Status::OK;
  }

  Status UpdateFriendRequestStatus(ServerContext*,
                                    const UpdateFriendRequestStatusReq* req,
                                    UpdateFriendRequestStatusRsp* rsp) override {
    bool ok = proxy_->updateFriendRequestStatus(req->id(), req->status());
    rsp->set_success(ok);
    rsp->set_message(ok ? "ok" : "update failed");
    return Status::OK;
  }

  // ========== 好友关系 ==========

  Status CreateFriendship(ServerContext*,
                           const CreateFriendshipReq* req,
                           CreateFriendshipRsp* rsp) override {
    bool ok = proxy_->createFriendship(req->user_id(), req->friend_id(),
                                        req->friend_username());
    rsp->set_success(ok);
    rsp->set_message(ok ? "ok" : "create friendship failed");
    return Status::OK;
  }

  Status GetFriendships(ServerContext*,
                         const GetFriendshipsReq* req,
                         GetFriendshipsRsp* rsp) override {
    auto friendships = proxy_->getFriendships(req->user_id());
    for (auto& f : friendships)
      *rsp->add_friendships() = std::move(f);
    rsp->set_message("ok");
    return Status::OK;
  }

  Status DeleteFriendship(ServerContext*,
                           const DeleteFriendshipReq* req,
                           DeleteFriendshipRsp* rsp) override {
    bool ok = proxy_->deleteFriendship(req->user_id(), req->friend_id());
    rsp->set_success(ok);
    rsp->set_message(ok ? "ok" : "delete failed");
    return Status::OK;
  }

  // ========== 用户查询 ==========

  Status GetUserIdByUsername(ServerContext*,
                              const GetUserIdByUsernameReq* req,
                              GetUserIdByUsernameRsp* rsp) override {
    auto [found, id] = proxy_->getUserIdByUsername(req->username());
    rsp->set_found(found);
    if (found) rsp->set_user_id(id);
    rsp->set_message(found ? "ok" : "not found");
    return Status::OK;
  }

  // ========== 消息记录 ==========

  Status BatchCreateMessages(ServerContext*,
                              const BatchCreateMessagesReq* req,
                              BatchCreateMessagesRsp* rsp) override {
    auto msgs = std::vector<db::MessageRecord>(req->messages().begin(), req->messages().end());
    int64_t n = proxy_->batchCreateMessages(msgs);
    if (n >= 0) {
      rsp->set_affected_count(static_cast<int32_t>(n));
      rsp->set_message("ok");
    } else {
      rsp->set_affected_count(0);
      rsp->set_message("batch create messages failed");
    }
    return Status::OK;
  }

  Status GetMessagesBetween(ServerContext*,
                             const GetMessagesBetweenReq* req,
                             GetMessagesBetweenRsp* rsp) override {
    auto msgs = proxy_->getMessagesBetween(
        req->user_id_a(), req->user_id_b(), req->limit(), req->offset());
    for (auto& m : msgs)
      *rsp->add_messages() = std::move(m);
    rsp->set_message("ok");
    return Status::OK;
  }
};

int main() {
  MySQLProxy proxy;
  if (!proxy.connect("127.0.0.1", 3306, "root", "123456", "test_db")) {
    fprintf(stderr, "MySQL connect failed, exiting\n");
    return 1;
  }
  if (!proxy.initSchema("test_db")) {
    fprintf(stderr, "Schema init failed, exiting\n");
    return 1;
  }

  std::string addr("0.0.0.0:50052");
  DBServiceImpl svc(&proxy);
  ServerBuilder builder;
  builder.AddListeningPort(addr, InsecureServerCredentials());
  builder.RegisterService(&svc);
  auto server = builder.BuildAndStart();
  printf("DB proxy server listening on %s\n", addr.c_str());
  server->Wait();
  return 0;
}
