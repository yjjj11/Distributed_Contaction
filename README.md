# Distributed_Contaction

一个基于 **gRPC 微服务架构**的分布式即时通讯（IM）系统。服务端全部用 **C++17** 实现，网关用 **Go**，前端为原生 HTML/CSS/JS。

> ⚠️ 学习项目：用于练习 gRPC、分布式服务拆分、网络编程与中间件使用，请勿直接用于生产环境。

---

## 🏗️ 系统架构

服务拆分（职责单一、可独立部署）：

```
                    ┌──────────────┐
   浏览器 / 前端  ──▶│  gateway     │  REST API 网关 (Go, :8080)
                    └──────┬───────┘
                           │ gRPC
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
 ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
 │ grpc_server │   │ auth_server │   │ db_server   │
 │ 登录/注册   │   │ JWT 认证     │   │ 数据库代理  │
 │ (:50051)    │   │ (:50053)    │   │ (:50052)    │
 └─────┬───────┘   └──────┬──────┘   └──────┬──────┘
       │ gRPC             │ Redis           │ MySQL
       │                  ▼                 ▼
       │           ┌─────────────┐   ┌─────────────┐
       └──────────▶│ msg_server  │   │ 用户/好友/  │
                   │ 消息服务     │   │ 消息记录    │
                   │ gRPC:50054   │   └─────────────┘
                   │ WS:50055     │
                   └──────┬──────┘
                          │
                    ┌─────▼─────┐
                    │  Redis    │  在线状态 / 离线消息
                    └───────────┘
```

### 服务一览

| 服务 | 语言 | 端口 | 职责 |
|------|------|------|------|
| `gateway` | Go | 8080 | REST API 网关，转发 HTTP → gRPC |
| `grpc_server` | C++ | 50051 | 注册 / 登录（双向流式 RPC）/ 改密码 / 注销账号 |
| `db_server` | C++ | 50052 | MySQL 代理，批量 CRUD、好友关系、消息记录 |
| `auth_server` | C++ | 50053 | JWT 签发 / 校验 / 吊销（OpenSSL + Redis） |
| `msg_server` | C++ | 50054 / 50055 | 在线消息推送（WebSocket）、离线消息（Redis）、批量落库 |

---

## ✨ 功能特性

- **账号体系**：注册、登录（gRPC 双向流）、修改密码、注销账号
- **JWT 认证**：`auth_server` 用 OpenSSL HMAC-SHA256 签发/校验 JWT，`gateway` 统一校验 Bearer Token / Cookie
- **好友系统**：发送好友请求、处理请求（接受/拒绝）、好友列表
- **实时聊天**：`msg_server` 用 Boost.Beast 提供 WebSocket 在线推送
- **离线消息**：离线消息写入 Redis，上线后拉取（Pull）并 Ack
- **历史消息**：按好友分页查询聊天记录
- **批量落库**：消息攒批写入 MySQL（阈值 50 条 / 5 秒间隔），减少高频写库压力
- **REST 网关**：Go 实现统一 REST 接口，前端只对接 HTTP
- **一键启停**：`start_all.sh` / `stop_all.sh` 管理全部服务

---

## 📁 目录结构

```
├── proto/                  # gRPC / Protobuf 接口定义
│   ├── login.proto         # 登录注册服务
│   ├── auth.proto          # JWT 认证服务
│   ├── db.proto            # 数据库代理服务
│   └── msg.proto           # 消息服务
├── server/                 # C++ 服务端实现
│   ├── server.cpp          # 登录服务
│   ├── auth_server.cpp     # JWT 认证服务
│   ├── db_server.cpp       # 数据库服务
│   ├── db_proxy.cpp/.hpp   # 数据库代理（抽象接口 + MySQL 实现）
│   └── msg_server.cpp      # 消息服务（gRPC + WebSocket + Redis）
├── gateway/                # Go REST 网关
│   ├── main.go
│   └── pb/                 # 生成的 Go 代码
├── frontend/               # 原生前端（HTML/CSS/JS）
├── third_party/hiredis/    # 内置 hiredis
├── CMakeLists.txt          # CMake 构建脚本
├── start_all.sh            # 一键启动所有服务
└── stop_all.sh             # 一键停止所有服务
```

---

## 🔧 依赖

- C++17 编译器、CMake ≥ 3.22、LLD 链接器
- gRPC / Protobuf / abseil
- OpenSSL（JWT 签名）、MySQL client、Boost（json / system / headers）、hiredis
- Go（构建 gateway）

## 🚀 快速开始

```bash
# 1. 构建
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# 2. 启动（自动拉起 Redis、启动全部服务）
./start_all.sh

# 3. 访问前端
# 打开 frontend/index.html 或通过网关 :8080 访问
```

启动成功后各服务监听：

```
db_server  :50052
auth_server:50053
grpc_server:50051
msg_server :50054(gRPC)  :50055(WebSocket)
gateway    :8080(REST API)
```

停止全部服务：`./stop_all.sh`

---

## 🧠 设计要点 / 收获

1. **服务拆分**：登录、认证、消息、数据库各自独立成 gRPC 服务，体会"微服务"如何拆分与协作。
2. **跨语言通信**：C++ 服务端 + Go 网关，同一份 `.proto` 生成多种语言代码，gRPC 跨语言互通的实践。
3. **认证方案**：自实现 JWT（HMAC-SHA256）签发与校验，配合 Redis 做 token 吊销（黑名单）。
4. **在线 / 离线**：在线走 WebSocket 实时推送，离线消息暂存 Redis 待上线拉取，是 IM 的经典做法。
5. **批量写库**：高频消息攒批后批量写入 MySQL，体会"批量 vs 单条"对数据库压力的差异。
6. **前后端分离**：前端只对接 REST，网关负责鉴权与协议转换。

---

## 📄 License

学习用途项目，无开源许可声明。
