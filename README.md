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
| CompanyInfo | 上市/上櫃公司基本資料與產業對照表 |
| OilPrice | 國際原油價格（WTI 西德州 + Brent 布蘭特） |
| GoldPrice | 國際黃金價格（COMEX Gold Futures） |
| BitcoinPrice | 比特幣價格（BTC-USD） |
| CurrencyPrice | 國際匯率（USDTWD + JPYTWD） |

## 安裝

### 直接安裝套件
```bash
pip install -r requirements.txt
pip install .
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

# 工商時報台股新聞（日期模式）
df_ctee = tw_crawler.ctee_news_crawler("2024-10-15")

# 鉅亨網台股新聞（日期模式）
df_cnyes = tw_crawler.cnyes_news_crawler("2024-10-15")

# PTT 股版文章（日期模式）
df_ptt = tw_crawler.ptt_news_crawler("2024-10-15")

# 聯合新聞網經濟日報台股新聞（日期模式）
df_moneyudn = tw_crawler.moneyudn_news_crawler("2024-10-15")

# 新聞爬蟲支援「時數模式」——抓取過去 N 小時的新聞
df_cnyes_24h = tw_crawler.cnyes_news_crawler("2024-10-15", hours=24)

# 上市/上櫃公司基本資料與產業對照表（不需要 date 參數）
result = tw_crawler.company_info_crawler()
company_info = result["company_info"]   # 公司基本資料列表
industry_map = result["industry_map"]   # 產業代碼對照表

# 國際原油價格（WTI + Brent）
oil_prices = tw_crawler.oil_price_crawler("2024-10-15")
# 回傳 list[dict]，每筆包含 product, date, open, high, low, close, volume

# 國際黃金價格（COMEX Gold Futures）
gold_prices = tw_crawler.gold_price_crawler("2024-10-15")
# 回傳 list[dict]，格式同 oil_price_crawler

# 比特幣價格（BTC-USD）
btc_prices = tw_crawler.bitcoin_price_crawler("2024-10-15")
# 回傳 list[dict]，格式同 oil_price_crawler

# 國際匯率（USDTWD + JPYTWD）
currency_prices = tw_crawler.currency_price_crawler("2024-10-15")
# 回傳 list[dict]，JPYTWD 若無直接 ticker 會自動從 TWD=X / JPY=X 交叉計算
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
| `GET /oil_price` | 國際原油價格（WTI + Brent） |
| `GET /gold_price` | 國際黃金價格（COMEX Gold Futures） |
| `GET /bitcoin_price` | 比特幣價格（BTC-USD） |
| `GET /currency_price` | 國際匯率（USDTWD + JPYTWD） |
| `GET /company_info` | 上市/上櫃公司基本資料與產業對照表 |

除 `/company_info` 外，所有 endpoint 皆支援 `?date=YYYY-MM-DD` 參數，不帶參數則預設為當天。`/oil_price`、`/gold_price`、`/bitcoin_price`、`/currency_price` 若查詢日期為非交易日，會自動回傳最近一個交易日的資料。

新聞端點（`/ctee_news`、`/cnyes_news`、`/ptt_news`、`/moneyudn_news`）額外支援 `?hours=N` 參數（1-72），可抓取過去 N 小時內的新聞，避免排程間隔漏抓。`hours` 優先於 `date`，同時指定時以 `hours` 為準。

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

# 爬取過去 24 小時的鉅亨網新聞
response = requests.get("http://127.0.0.1:6738/cnyes_news?hours=24")
data = response.json()

# 爬取公司基本資料與產業對照表
response = requests.get("http://127.0.0.1:6738/company_info")
data = response.json()

# 爬取國際原油價格
response = requests.get("http://127.0.0.1:6738/oil_price?date=2024-10-15")
data = response.json()

# 爬取國際黃金價格
response = requests.get("http://127.0.0.1:6738/gold_price?date=2024-10-15")
data = response.json()

# 爬取比特幣價格
response = requests.get("http://127.0.0.1:6738/bitcoin_price?date=2024-10-15")
data = response.json()

# 爬取國際匯率
response = requests.get("http://127.0.0.1:6738/currency_price?date=2024-10-15")
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

## CI/CD

本專案使用 GitHub Actions 自動建置並發布 Docker image 至 DockerHub。

### 觸發條件

當推送符合 `v*.*.*` 格式的 git tag 時自動觸發（例如 `v1.0.0`、`v2.1.3`）。

### Pipeline 流程

1. Checkout 程式碼
2. 從 tag 名稱擷取版本號（去除 `v` 前綴）
3. 登入 DockerHub
4. 建置 Docker image 並推送至 DockerHub，同時標記版本號與 `latest`

### 發布新版本

```bash
# 建立 tag 並推送至 remote，觸發自動建置
git tag v1.0.0
git push origin v1.0.0
```

推送後 GitHub Actions 會自動建置並發布：

- `nk7260ynpa/tw_stocker_crawler:<版本號>`（例如 `1.0.0`）
- `nk7260ynpa/tw_stocker_crawler:latest`

### 必要的 GitHub Secrets

| Secret | 說明 |
|--------|------|
| `DOCKERHUB_USERNAME` | DockerHub 帳號 |
| `DOCKERHUB_TOKEN` | DockerHub Access Token |

## Log

Log 檔案儲存於 `logs/` 資料夾，按日期自動輪替（保留 30 天）：
- 當天：`logs/crawler.log`
- 歷史：`logs/crawler.YYYY-MM-DD.log`

## CHANGELOG

### v2.9.0
- 新增國際黃金價格爬蟲（GoldPrice），使用 yfinance 取得 COMEX Gold Futures 價格
- 新增比特幣價格爬蟲（BitcoinPrice），使用 yfinance 取得 BTC-USD 價格
- 新增國際匯率爬蟲（CurrencyPrice），支援 USDTWD 與 JPYTWD，JPYTWD 支援 fallback 交叉計算
- 新增 /gold_price、/bitcoin_price、/currency_price 三個 API endpoint

### v2.8.0
- 新增國際原油價格爬蟲（OilPrice），使用 yfinance 取得 WTI 與 Brent 原油期貨價格
- 新增 /oil_price API endpoint，支援 `?date=YYYY-MM-DD` 查詢參數
- 假日查詢時自動回傳最近交易日的資料
- 新增 yfinance 依賴

### v2.7.1
- PTT 股版爬蟲新增 HTTP 請求重試機制（指數退避），修復 SSL 間歇性錯誤導致排程全部失敗的問題
- `fetch_list_page` 和 `fetch_article_detail` 遇到 SSL/連線錯誤時自動重試最多 3 次（延遲 1s, 2s, 4s）

### v2.7.0
- 新增公司產業對照爬蟲（CompanyInfo），從 TWSE/TPEX OpenAPI 動態取得公司基本資料
- 新增 /company_info API endpoint，回傳公司資訊與產業代碼對照表
- 支援 NormalShares 自動計算（實收資本額 / 面額 - 特別股 - 私募股數）
- 內建 TWSE/TPEX 產業代碼對照表，自動映射產業名稱

### v2.6.0
- 新聞爬蟲新增「時數模式」（`hours` 參數），支援抓取過去 N 小時內的新聞
- 四個新聞 API 端點（/ctee_news, /cnyes_news, /ptt_news, /moneyudn_news）新增 `?hours=N` 查詢參數
- 時數模式下文章 Date 欄位使用文章自身的發布日期（可能跨日）
- 向下相容：不帶 hours 參數時行為與原來完全相同

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
