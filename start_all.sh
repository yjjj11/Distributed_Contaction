#!/bin/bash
# 启动所有服务
DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="$DIR/bin"

echo "=== 启动所有服务 ==="

# 清理旧进程
pkill -f db_server 2>/dev/null
pkill -f auth_server 2>/dev/null
pkill -f grpc_server 2>/dev/null
pkill -f msg_server 2>/dev/null
pkill -f gateway 2>/dev/null
sleep 1

# 0. Redis（如果未运行）
if ! pgrep -x redis-server > /dev/null; then
    redis-server --daemonize yes 2>/dev/null
    echo "[Redis] started"
    sleep 1
fi

# 1. db_server
$BIN_DIR/db_server &
echo "[DB] started on :50052"

# 2. auth_server
$BIN_DIR/auth_server &
echo "[Auth] started on :50053"

sleep 2

# 3. grpc_server（登录/注册）
$BIN_DIR/grpc_server --port=50051 &
echo "[gRPC] started on :50051"

sleep 1

# 4. msg_server（WebSocket + 消息服务）
$BIN_DIR/msg_server &
echo "[Msg] started on gRPC:50054 WS:50055"

sleep 1

# 5. gateway（REST API）
export MSG_WS_ADDR="ws://localhost:50055/ws"
$BIN_DIR/gateway &
echo "[Gateway] started on :8080"

echo ""
echo "=== 所有服务已启动 ==="
echo "  db_server  :50052"
echo "  auth_server:50053"
echo "  grpc_server:50051"
echo "  msg_server :50054(gRPC) :50055(WS)"
echo "  gateway    :8080"
