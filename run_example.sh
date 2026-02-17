#!/bin/bash
# 在 Docker container 中執行 api_example.py 測試本地 API

IMAGE_NAME="nk7260ynpa/tw_stocker_crawler:latest"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

docker run --rm \
    -v "$SCRIPT_DIR/api_example.py:/workspace/api_example.py" \
    --network host \
    -e API_BASE_URL="http://127.0.0.1:6738" \
    "$IMAGE_NAME" \
    python api_example.py
