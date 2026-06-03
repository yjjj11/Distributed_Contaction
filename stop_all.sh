#!/bin/bash
echo "=== 停止所有服务 ==="
pkill -f msg_server 2>/dev/null && echo "[Msg] stopped" || echo "[Msg] not running"
pkill -f grpc_server 2>/dev/null && echo "[gRPC] stopped" || echo "[gRPC] not running"
pkill -f auth_server 2>/dev/null && echo "[Auth] stopped" || echo "[Auth] not running"
pkill -f db_server 2>/dev/null && echo "[DB] stopped" || echo "[DB] not running"
pkill -f gateway 2>/dev/null && echo "[Gateway] stopped" || echo "[Gateway] not running"
echo "=== 完成 ==="
