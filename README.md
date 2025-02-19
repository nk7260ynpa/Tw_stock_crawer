# Tw_stock_crawer(台灣股市爬蟲)
![](https://img.shields.io/static/v1?label=python&message=3.8>=&color=blue)
[![](https://img.shields.io/static/v1?label=license&message=MIT&color=green)](./License.txt)

## 安裝
```bash
pip install -r requirements.txt
python setup.py install
```

## 使用

輸入日期，即可自動抓取當日上市、上櫃、興櫃的股票資料。日期格式為 `YYYY-MM-DD`。

以下為範例：

```python
import tw_stock_crawer

# 抓取當日上市股票資料
df_twse = tw_stock_crawer.twse_crawler("2024-10-15")

# 抓取當日上櫃股票資料
df_tpex = tw_stock_crawer.tpex_crawler("2024-10-15")

# 抓取當日興櫃股票資料
df_taifex = tw_stock_crawer.taifex_crawler("2024-10-15")
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
