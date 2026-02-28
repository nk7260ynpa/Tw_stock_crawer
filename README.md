# Tw_stock_crawer（台灣股市爬蟲）
![](https://img.shields.io/static/v1?label=python&message=3.8>=&color=blue)
[![](https://img.shields.io/static/v1?label=license&message=MIT&color=green)](./License.txt)

## 功能

提供台灣股市資料爬取功能，包含：

| 模組 | 說明 |
|------|------|
| TWSE | 上市股票 |
| TPEX | 上櫃股票 |
| TAIFEX | 期貨 |
| FAOI | 三大法人買賣超 |
| MGTS | 融資融券 |
| TDCC | 集保戶股權分散表（每週更新） |
| CTEE News | 工商時報台股新聞 |
| CNYES News | 鉅亨網台股新聞 |
| PTT News | PTT 股版文章 |
| MoneyUDN News | 聯合新聞網經濟日報台股新聞 |

## 安裝

### 直接安裝套件
```bash
pip install -r requirements.txt
python setup.py install
```

### 使用 Docker 環境
```bash
bash docker/build.sh
```

## 使用方式

### 方式一：Python 套件

輸入日期，即可自動抓取當日股票資料。日期格式為 `YYYY-MM-DD`。

```python
import tw_crawler

# 上市股票
df_twse = tw_crawler.twse_crawler("2024-10-15")

# 上櫃股票
df_tpex = tw_crawler.tpex_crawler("2024-10-15")

# 期貨
df_taifex = tw_crawler.taifex_crawler("2024-10-15")

# 三大法人
df_faoi = tw_crawler.faoi_crawler("2024-10-15")

# 融資融券
df_mgts = tw_crawler.mgts_crawler("2024-10-15")

# 集保戶股權分散表（回傳最新一期資料，date 參數不影響查詢結果）
df_tdcc = tw_crawler.tdcc_crawler("2024-10-15")

# 工商時報台股新聞
df_ctee = tw_crawler.ctee_news_crawler("2024-10-15")

# 鉅亨網台股新聞
df_cnyes = tw_crawler.cnyes_news_crawler("2024-10-15")

# PTT 股版文章
df_ptt = tw_crawler.ptt_news_crawler("2024-10-15")

# 聯合新聞網經濟日報台股新聞
df_moneyudn = tw_crawler.moneyudn_news_crawler("2024-10-15")
```

### 方式二：FastAPI Server

啟動 Docker container：

```bash
bash run.sh
```

Server 預設運行於 `http://127.0.0.1:6738/`。

#### API Endpoints

| Endpoint | 說明 |
|----------|------|
| `GET /` | 並行爬取所有資料 |
| `GET /twse` | 上市股票 |
| `GET /tpex` | 上櫃股票 |
| `GET /taifex` | 期貨 |
| `GET /faoi` | 三大法人 |
| `GET /mgts` | 融資融券 |
| `GET /tdcc` | 集保戶股權分散表（僅回傳最新一期） |
| `GET /ctee_news` | 工商時報台股新聞 |
| `GET /cnyes_news` | 鉅亨網台股新聞 |
| `GET /ptt_news` | PTT 股版文章 |
| `GET /moneyudn_news` | 聯合新聞網經濟日報台股新聞 |

所有 endpoint 皆支援 `?date=YYYY-MM-DD` 參數，不帶參數則預設為當天。

> **注意**：TDCC 集保資料由 API 固定回傳最新一期（通常每週五更新），`date` 參數不影響查詢結果。

#### 請求範例

```python
import requests

# 爬取指定日期的上市股票資料
response = requests.get("http://127.0.0.1:6738/twse?date=2024-10-15")
data = response.json()

# 爬取當天所有資料
response = requests.get("http://127.0.0.1:6738/")
data = response.json()
```

#### API 測試腳本

在 Docker 內執行範例腳本，測試所有 endpoint：

```bash
bash run_example.sh
```

## 測試

在 Docker 中執行測試：
```bash
docker run --rm nk7260ynpa/tw_stocker_crawler:latest pytest test/ -v
```

包含覆蓋率：
```bash
docker run --rm nk7260ynpa/tw_stocker_crawler:latest pytest --cov-report term-missing --cov-config=.coveragerc --cov=./tw_crawler test/
```

## Log

Log 檔案儲存於 `logs/` 資料夾，按日期自動輪替（保留 30 天）：
- 當天：`logs/crawler.log`
- 歷史：`logs/crawler.YYYY-MM-DD.log`

## CHANGELOG

### v2.5.0
- 新增 MoneyUDN 聯合新聞網經濟日報台股新聞爬蟲（requests + BeautifulSoup + markdownify）
- 新增 /moneyudn_news API endpoint
- 使用 JSON-LD 結構化資料解析列表頁，保留文章圖片為 Markdown 格式

### v2.4.0
- 新增 PTT 股版新聞爬蟲（requests + BeautifulSoup + markdownify）
- 新增 /ptt_news API endpoint
- PTT 爬蟲支援年齡驗證 cookie、反向翻頁、文章頁面日期二次驗證

### v2.3.0
- 新增 CNYES 鉅亨網台股新聞爬蟲
- 新增 /cnyes_news API endpoint
- 新增 markdownify 依賴（用於 HTML 轉 Markdown）

### v2.2.0
- 新增 CTEE 工商時報台股新聞爬蟲
- 新增 /ctee_news API endpoint
- 新增 beautifulsoup4、lxml 依賴

### v2.1.0
- 新增 TDCC 集保戶股權分散表爬蟲
- 新增 /tdcc API endpoint
- ThreadPoolExecutor 更新為 6 個 workers

### v2.0.0
- 新增 FastAPI Server，提供 REST API 介面
- 新增各爬蟲獨立 endpoint（/twse, /tpex, /taifex, /faoi, /mgts）
- 支援 date 查詢參數指定日期
- Log 按日期自動輪替儲存
- Docker 掛載 logs 資料夾

### v1.4.1
- 修正融資融券爬蟲錯誤

### v1.4.0
- 加入融資融券爬蟲

### v1.3.0
- 加入三大法人爬蟲

### v1.2.1
- 加入 logger 功能，方便追蹤錯誤
