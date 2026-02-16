# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 專案概述

台灣股市爬蟲套件（tw_crawler），提供上市(TWSE)、上櫃(TPEX)、興櫃(TAIFEX)、三大法人(FAOI)、融資融券(MGTS) 的每日股票資料爬取功能。同時提供 FastAPI server 作為 API 介面。

## 常用指令

### 建置與安裝
```bash
# Docker 建置
bash docker/build.sh

# 本地安裝
pip install -r requirements.txt
python setup.py install
```

### 測試
```bash
# 執行所有測試
pytest

# 含覆蓋率
pytest --cov-report term-missing --cov-config=.coveragerc --cov=./tw_crawler test/

# 執行單一測試檔
pytest test/test_twse.py
```

### 啟動服務
```bash
# Docker 方式（port 6738）
docker run -d --name tw_stocker_crawler -p 6738:6738 --rm nk7260ynpa/tw_stocker_crawler:latest
```

## 架構

- `tw_crawler/` — 核心爬蟲模組，每個爬蟲獨立一個檔案（twse.py, tpex.py, taifex.py, faoi.py, mgts.py），透過 `__init__.py` 統一匯出
- `server.py` — FastAPI server，接受 name（爬蟲名稱）和 date（日期 YYYY-MM-DD）參數
- `test/` — pytest 測試，對應各爬蟲模組
- `docker/` — Dockerfile 與 build.sh

## 技術細節

- Python >= 3.8，Docker 基於 python:3.12.7
- 使用 cloudscraper（非 requests）處理部分有 Cloudflare 保護的網站
- 爬蟲函式統一介面：傳入日期字串（YYYY-MM-DD），回傳 pandas DataFrame
- 每個爬蟲模組包含 `zh2en_columns` 和 `post_process` 做欄位中英對照與資料清洗
