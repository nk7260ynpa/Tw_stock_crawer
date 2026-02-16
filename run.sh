#!/bin/bash
# 啟動 Docker container 並執行 FastAPI server

IMAGE_NAME="nk7260ynpa/tw_stocker_crawler:latest"
CONTAINER_NAME="tw_stocker_crawler"
PORT=6738

# 移除舊的 container（如果存在）
docker rm -f "$CONTAINER_NAME" 2>/dev/null

# 啟動 container
docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$PORT:$PORT" \
    --rm \
    "$IMAGE_NAME"

echo "Server started at http://127.0.0.1:$PORT/"
