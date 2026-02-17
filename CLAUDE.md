# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 專案概述

台灣股市爬蟲套件（tw_crawler），提供上市(TWSE)、上櫃(TPEX)、期貨(TAIFEX)、三大法人(FAOI)、融資融券(MGTS) 的每日資料爬取功能。同時提供 FastAPI server（port 6738）作為 REST API 介面，支援各爬蟲獨立或並行呼叫。

## 常用指令

### 建置
```bash
bash docker/build.sh
```

### 測試（在 Docker 中執行）
```bash
# 執行所有測試
docker run --rm nk7260ynpa/tw_stocker_crawler:latest pytest test/ -v

# 含覆蓋率
docker run --rm nk7260ynpa/tw_stocker_crawler:latest pytest --cov-report term-missing --cov-config=.coveragerc --cov=./tw_crawler test/

# 執行單一測試檔
docker run --rm nk7260ynpa/tw_stocker_crawler:latest pytest test/test_twse.py -v
```

### 啟動服務
```bash
bash run.sh
# Server: http://127.0.0.1:6738/
```

### API 測試範例
```bash
bash run_example.sh
```

## 架構

- `tw_crawler/` — 核心爬蟲模組，每個爬蟲獨立一個檔案，透過 `__init__.py` 統一匯出
- `server.py` — FastAPI server，提供 `/`（全部並行）及 `/twse`、`/tpex`、`/taifex`、`/faoi`、`/mgts` 獨立 endpoint，皆支援 `?date=YYYY-MM-DD` 參數
- `test/` — pytest 測試，每個爬蟲模組及 server 各有對應測試檔
- `docker/` — Dockerfile 與 build.sh
- `logs/` — 日誌檔案，按日期自動輪替（crawler.YYYY-MM-DD.log）

## 技術細節

- Python >= 3.8，Docker 基於 python:3.12.7
- 使用 cloudscraper（非 requests）處理 TPEX、TAIFEX 等有 Cloudflare 保護的網站；TWSE、FAOI、MGTS 使用 requests
- 爬蟲函式統一介面：傳入日期字串（YYYY-MM-DD），回傳 pandas DataFrame
- 每個爬蟲模組包含 `zh2en_columns`（或 `webzh2en_columns`）和 `post_process` 做欄位中英對照與資料清洗
- `GET /` 使用 `ThreadPoolExecutor(max_workers=5)` 並行爬取全部 5 個資料源
- Log 使用 `TimedRotatingFileHandler`，每日午夜輪替，保留 30 天
