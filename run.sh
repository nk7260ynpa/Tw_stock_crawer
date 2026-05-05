#!/bin/bash
# 啟動 Docker container 並執行 FastAPI server
# 注意：此容器僅透過 db_network 在內部網路中提供服務，
# 不再對外公開 6738 port，其他 tw_stock 容器請以容器名 tw_stocker_crawler 存取。

IMAGE_NAME="nk7260ynpa/tw_stocker_crawler:latest"
CONTAINER_NAME="tw_stocker_crawler"
PORT=6738
LOG_DIR="$(cd "$(dirname "$0")" && pwd)/logs"

# 建立 logs 資料夾（如果不存在）
mkdir -p "$LOG_DIR"

# 移除舊的 container（如果存在）
docker rm -f "$CONTAINER_NAME" 2>/dev/null

# 啟動 container（不對外公開 port，僅限 db_network 內部存取）
docker run -d \
    --name "$CONTAINER_NAME" \
    --network db_network \
    --dns 8.8.8.8 \
    --dns 8.8.4.4 \
    -v "$LOG_DIR:/workspace/logs" \
    --restart=always \
    "$IMAGE_NAME"

echo "Server started (internal-only) at http://${CONTAINER_NAME}:${PORT}/ (db_network)"
