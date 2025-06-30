# Tw_stock_crawer(台灣股市爬蟲)
![](https://img.shields.io/static/v1?label=python&message=3.8>=&color=blue)
[![](https://img.shields.io/static/v1?label=license&message=MIT&color=green)](./License.txt)

## 安裝

### 直接使用安裝套件
```bash
pip install -r requirements.txt
python setup.py install
```

### 使用docker環境
```bash
docker run --rm -it  nk7260ynpa/tw_stocker_crawler  bash
```

## 使用-1

輸入日期，即可自動抓取當日上市、上櫃、興櫃的股票資料。日期格式為 `YYYY-MM-DD`。

以下為範例：

```python
import tw_crawler

# 抓取當日上市股票資料
df_twse = tw_crawler.twse_crawler("2024-10-15")

# 抓取當日上櫃股票資料
df_tpex = tw_crawler.tpex_crawler("2024-10-15")

# 抓取當日興櫃股票資料
df_taifex = tw_crawler.taifex_crawler("2024-10-15")

# 抓取當日融資融券資料
df_mgts = tw_crawler.mgts_crawler("2024-10-15")

# 抓取當日三大法人資料
df_faoi = tw_crawler.faoi_crawler("2024-10-15")
```

## 使用-2
1. 啟動Fastapi server，並提供API接口。

```bash
docker run -d --name tw_stocker_crawler -p 6738:6738 --rm nk7260ynpa/tw_stocker_crawler:latest
```
2. 使用curl或Postman等工具，向API發送請求。
```python
import requests
import pandas as pd

name = "twse"
date = "2025-05-28"

url = "http://127.0.0.1:6738"
payload = {"name": name, "date": date}

response = requests.post(url, params=payload)
df = pd.DataFrame(response.json()["data"])

print(df.head(2))
```

## 測試
普通測試
```bash
pytest
```
包含覆蓋率的測試
```bash
pytest --cov-report term-missing --cov-config=.coveragerc --cov=./tw_crawler test/
```

# CHANGELOG
## 版本更新
### v1.4.1
- 修正融資融券爬蟲錯誤。
### v1.4.0
- 加入融資融券爬蟲
### v1.3.0
- 加入三大法人爬蟲
### v1.2.1
- 加入logger功能，方便追蹤錯誤。