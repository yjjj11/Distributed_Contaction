#include <grpcpp/grpcpp.h>
#include "login.grpc.pb.h"
#include "db.grpc.pb.h"
#include "auth.grpc.pb.h"
#include <memory>
#include <string>
#include <tuple>

using namespace grpc;
using namespace login;

// 返回值： (是否成功, 消息, user_id)
using VerifyResult = std::tuple<bool, std::string, int64_t>;

class LoginServiceImpl final : public LoginService::Service {
  std::unique_ptr<db::DatabaseService::Stub> stub_;
  std::unique_ptr<auth::AuthService::Stub> auth_stub_;

  // 创建用户 —— 只填写 username, password, status=1
  bool CreateUser(const std::string& username, const std::string& password) {
    db::BatchCreateRequest req;
    auto* u = req.add_users();
    u->set_username(username);
    u->set_password(password);
    u->set_status(1);

    grpc::ClientContext ctx;
    db::BatchCreateResponse rsp;
    auto s = stub_->BatchCreate(&ctx, req, &rsp);
    return s.ok() && rsp.affected_count() > 0;
  }

  // 检查用户名是否已存在
  bool UserExists(const std::string& username) {
    db::QueryRequest req;
    req.set_field("username");
    req.set_value(username);
    req.set_page(1);
    req.set_page_size(1);

    grpc::ClientContext ctx;
    db::QueryResponse rsp;
    auto s = stub_->Query(&ctx, req, &rsp);
    return s.ok() && rsp.total() > 0 && rsp.users_size() > 0;
  }

  // 验证用户登录 —— 返回 (是否成功, 消息, user_id)
  VerifyResult VerifyUser(const std::string& username,
                          const std::string& password) {
    db::QueryRequest req;
    req.set_field("username");
    req.set_value(username);
    req.set_page(1);
    req.set_page_size(1);

    grpc::ClientContext ctx;
    db::QueryResponse rsp;
    auto s = stub_->Query(&ctx, req, &rsp);

    if (!s.ok()) {
      return {false, "db service unavailable", 0};
    }
    if (rsp.total() == 0 || rsp.users_size() == 0) {
      return {false, "Invalid username or password", 0};
    }
    const auto& user = rsp.users(0);
    if (user.password() != password) {
      return {false, "Invalid username or password", 0};
    }
    return {true, "ok", user.id()};
  }

  // 调用 Auth 服务创建 JWT
  std::string CreateToken(int64_t user_id, const std::string& username) {
    grpc::ClientContext ctx;
    auth::CreateTokenRequest req;
    req.set_user_id(user_id);
    req.set_username(username);
    auth::CreateTokenResponse rsp;
    auto s = auth_stub_->CreateToken(&ctx, req, &rsp);
    if (s.ok() && rsp.success()) {
      return rsp.token();
    }
    return "";
  }

  // 调用 Auth 服务吊销 token
  bool RevokeUserToken(int64_t user_id, int64_t ttl_seconds) {
    grpc::ClientContext ctx;
    auth::RevokeTokenRequest req;
    req.set_user_id(user_id);
    req.set_ttl_seconds(ttl_seconds);
    auth::RevokeTokenResponse rsp;
    auto s = auth_stub_->RevokeToken(&ctx, req, &rsp);
    return s.ok() && rsp.success();
  }

 public:
  LoginServiceImpl(std::shared_ptr<grpc::Channel> db_channel,
                   std::shared_ptr<grpc::Channel> auth_channel)
      : stub_(db::DatabaseService::NewStub(db_channel)),
        auth_stub_(auth::AuthService::NewStub(auth_channel)) {}

  Status Register(ServerContext*, const RegisterRequest* req,
                  RegisterReply* rsp) override {
    if (UserExists(req->username())) {
      rsp->set_success(false);
      rsp->set_message("username already exists");
      return Status::OK;
    }
    if (CreateUser(req->username(), req->password())) {
      rsp->set_success(true);
      rsp->set_message("register success");
    } else {
      rsp->set_success(false);
      rsp->set_message("register failed");
    }
    return Status::OK;
  }

  Status Login(ServerContext* ctx,
               grpc::ServerReaderWriter<LoginReply, LoginRequest>* stream)
      override {
    LoginRequest req;
    LoginReply rsp;
    while (stream->Read(&req)) {
      auto [ok, msg, user_id] = VerifyUser(req.username(), req.password());
      if (!ok) {
        rsp.set_success(false);
        rsp.set_message(msg);
        stream->Write(rsp);
        continue;
      }
      // 验证通过，创建 JWT
      std::string token = CreateToken(user_id, req.username());
      if (token.empty()) {
        rsp.set_success(false);
        rsp.set_message("token creation failed");
      } else {
        rsp.set_success(true);
        rsp.set_message("login success");
        rsp.set_token(token);
      }
      stream->Write(rsp);
    }
    return Status::OK;
  }

  Status ChangePassword(ServerContext*, const ChangePasswordRequest* req,
                        ChangePasswordReply* rsp) override {
    auto [ok, msg, user_id] = VerifyUser(req->username(), req->old_password());
    if (!ok) {
      rsp->set_success(false);
      rsp->set_message(msg);
      return Status::OK;
    }

    // 用 user_id 获取完整用户信息（避免重复 Query）
    db::BatchGetRequest bg_req;
    bg_req.add_ids(user_id);
    grpc::ClientContext bg_ctx;
    db::BatchGetResponse bg_rsp;
    auto s = stub_->BatchGet(&bg_ctx, bg_req, &bg_rsp);
    if (!s.ok() || bg_rsp.users_size() == 0) {
      rsp->set_success(false);
      rsp->set_message("user not found");
      return Status::OK;
    }

    db::User updated_user = bg_rsp.users(0);
    updated_user.set_password(req->new_password());

    db::BatchUpdateRequest up_req;
    *up_req.add_users() = updated_user;
    grpc::ClientContext up_ctx;
    db::BatchUpdateResponse up_rsp;
    auto s2 = stub_->BatchUpdate(&up_ctx, up_req, &up_rsp);
    if (s2.ok() && up_rsp.affected_count() > 0) {
      rsp->set_success(true);
      rsp->set_message("password changed");
    } else {
      rsp->set_success(false);
      rsp->set_message("password change failed");
    }
    return Status::OK;
  }

  Status DeleteAccount(ServerContext*, const DeleteAccountRequest* req,
                       DeleteAccountReply* rsp) override {
    // 验证密码，直接拿到 user_id
    auto [ok, msg, user_id] = VerifyUser(req->username(), req->password());
    if (!ok) {
      rsp->set_success(false);
      rsp->set_message(msg);
      return Status::OK;
    }

    // 按 id 删除
    db::BatchDeleteRequest del_req;
    del_req.add_ids(user_id);

    grpc::ClientContext del_ctx;
    db::BatchDeleteResponse del_rsp;
    auto s = stub_->BatchDelete(&del_ctx, del_req, &del_rsp);
    if (s.ok() && del_rsp.affected_count() > 0) {
      // 同时吊销该用户的 token
      RevokeUserToken(user_id, 86400);
      rsp->set_success(true);
      rsp->set_message("account deleted");
    } else {
      rsp->set_success(false);
      rsp->set_message("delete failed");
    }
    return Status::OK;
  }
};

void RunServer(const std::string& addr, const std::string& db_addr,
               const std::string& auth_addr) {
  auto db_channel = grpc::CreateChannel(db_addr,
      grpc::InsecureChannelCredentials());
  auto auth_channel = grpc::CreateChannel(auth_addr,
      grpc::InsecureChannelCredentials());

  LoginServiceImpl service(db_channel, auth_channel);

  ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  if (!server) {
    std::cerr << "Failed to start login server on " << addr << std::endl;
    return;
  }
  std::cout << "Login server listening on " << addr << std::endl;
  server->Wait();
}

int main(int argc, char** argv) {
  std::string addr = "0.0.0.0:50051";
  std::string db_addr = "localhost:50052";
  std::string auth_addr = "localhost:50053";
  for (int i = 1; i + 1 < argc; i += 2) {
    std::string key = argv[i];
    if (key == "--addr" || key == "-a") addr = argv[i + 1];
    else if (key == "--port" || key == "-p") addr = "0.0.0.0:" + std::string(argv[i + 1]);
    else if (key == "--db" || key == "-d") db_addr = argv[i + 1];
    else if (key == "--auth" || key == "-x") auth_addr = argv[i + 1];
  }
  RunServer(addr, db_addr, auth_addr);
  return 0;
}
