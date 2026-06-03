#include <grpcpp/grpcpp.h>
#include "login.grpc.pb.h"
#include <string>
#include <vector>
#include <memory>

using namespace grpc;
using namespace login;

static void test_registration(const std::unique_ptr<LoginService::Stub>& stub,
                              const std::string& user, const std::string& pass) {
  ClientContext ctx;
  RegisterRequest req;
  req.set_username(user);
  req.set_password(pass);
  RegisterReply rsp;
  Status s = stub->Register(&ctx, req, &rsp);
  printf("[register] %s / %s -> %s | %s\n",
         user.c_str(), pass.c_str(),
         rsp.success() ? "OK" : "FAIL",
         rsp.message().c_str());
}

int main() {
  auto stub = LoginService::NewStub(CreateChannel("127.0.0.1:50051", InsecureChannelCredentials()));

  // ---- 测试注册 ----
  printf("=== Register Tests ===\n");
  test_registration(stub, "dave",  "qwerty");
  test_registration(stub, "alice", "hijack");   // 已存在，应失败
  test_registration(stub, "",      "hijack");    // 空用户名，应失败
  test_registration(stub, "eve",   "");          // 空密码，应失败

  // ---- 登录 ----
  printf("\n=== Login Tests ===\n");
  {
    ClientContext ctx;
    auto stream = stub->Login(&ctx);

    std::vector<std::pair<std::string, std::string>> attempts = {
      {"alice",   "123456"},   // 预置用户，应成功
      {"bob",     "wrongpw"},  // 密码错误
      {"dave",    "qwerty"},   // 刚注册的用户，应成功
      {"eve",     ""},         // 空密码用户不存在
    };

    LoginRequest req;
    for (auto& [user, pass] : attempts) {
      req.set_username(user);
      req.set_password(pass);
      if (!stream->Write(req)) {
        printf("Write failed for %s\n", user.c_str());
        break;
      }
      printf("[client] sent login request for %s\n", user.c_str());
    }
    stream->WritesDone();

    LoginReply rsp;
    while (stream->Read(&rsp)) {
      printf("[client] result: %s  |  %s\n",
             rsp.success() ? "OK" : "FAIL",
             rsp.message().c_str());
      if (rsp.success() && !rsp.token().empty()) {
        printf("[client] token: %s\n", rsp.token().c_str());
      }
    }
    printf("[client] stream finished\n");
  }
  return 0;
}
