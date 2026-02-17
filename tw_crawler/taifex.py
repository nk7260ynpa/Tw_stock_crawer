"""TAIFEX 期貨爬蟲模組。

提供台灣期貨交易所(TAIFEX)每日期貨資料爬取與處理功能。
"""

import io

import cloudscraper
import numpy as np
import pandas as pd

def webzh2en_columns() -> dict[str, str]:
    """回傳中文欄位名稱對應英文欄位名稱的字典。

    Returns:
        dict[str, str]: 中英文欄位名稱對照字典。
    """
    webzh2en_columns = {
        "交易日期": "Date",
        "契約": "Contract",
        "到期月份(週別)": "ContractMonth",
        "開盤價": "Open",
        "最高價": "High",
        "最低價": "Low",
        "收盤價": "Last",
        "漲跌價": "Change",
        "漲跌%": "ChangePercent",
        "成交量": "Volume",
        "結算價": "SettlementPrice",
        "未沖銷契約數": "OpenInterest",
        "最後最佳買價": "BestBid",
        "最後最佳賣價": "BestAsk",
        "歷史最高價": "HistoricalHigh",
        "歷史最低價": "HistoricalLow",
        "是否因訊息面暫停交易": "TradingHalt",
        "交易時段": "TradingSession",
        "價差對單式委託成交量": "SpreadOrderVolume"
    }
    return webzh2en_columns

def post_process(df: pd.DataFrame) -> pd.DataFrame:
    """將從 TAIFEX 網站爬取的原始資料表做欄位轉換與清洗。

    Args:
        df: 從 TAIFEX 網站爬取的原始 DataFrame。

    Returns:
        處理後的 DataFrame。
    """
    df = df.rename(columns=webzh2en_columns())
    df["Date"] = pd.to_datetime(df["Date"], format="%Y/%m/%d")
    df["Contract"] = df["Contract"].astype(str)
    df["ContractMonth"] = df["ContractMonth"].astype(str)
    df["Open"] = df["Open"].replace("-", None).astype(float)
    df["High"] = df["High"].replace("-", None).astype(float)
    df["Low"] = df["Low"].replace("-", None).astype(float)
    df["Last"] = df["Last"].replace("-", None).astype(float)
    df["Change"] = df["Change"].replace("-", None).astype(float)
    df["ChangePercent"] = df["ChangePercent"].replace("-", None).str.replace("%", "").astype(float) / 100.0
    df["Volume"] = df["Volume"].astype(int)
    df["SettlementPrice"] = df["SettlementPrice"].replace("-", None).astype(float)
    df["OpenInterest"] = df["OpenInterest"].replace("-", None).astype(float)
    df["BestBid"] = df["BestBid"].replace("-", None).astype(float)
    df["BestAsk"] = df["BestAsk"].replace("-", None).astype(float)
    df["HistoricalHigh"] = df["HistoricalHigh"].replace("-", None).astype(float)
    df["HistoricalLow"] = df["HistoricalLow"].replace("-", None).astype(float)
    df["TradingHalt"] = df["TradingHalt"].replace("-", None).replace("*", None).replace(" ", "").map(lambda x: {"": None}.get(x, x)).replace("是", 1.0).replace("否", 0.0).astype(float)
    df["TradingSession"] = df["TradingSession"].astype(str)
    df["SpreadOrderVolume"] = df["SpreadOrderVolume"].astype(float)
    return df

def fetch_taifex_data(date: str) -> str:
    """從 TAIFEX 網站取得指定日期的期貨資料。

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        TAIFEX 回傳的 CSV 文字內容。
    """
    url = "https://www.taifex.com.tw/cht/3/futDataDown"
    date = date.replace("-", "/")
    payload = {
        "down_type": "1",
        "commodity_id": "all",
        "queryStartDate": date,
        "queryEndDate": date
    }
    scraper = cloudscraper.create_scraper()
    response = scraper.post(url, data=payload)
    response.raise_for_status()  # Ensure we raise an error for bad responses
    return response.text

def parse_taifex_data(response: str) -> pd.DataFrame:
    """將 TAIFEX 回傳的 CSV 文字解析為 DataFrame。

    Args:
        response: TAIFEX 回傳的 CSV 文字內容。

    Returns:
        解析後的 DataFrame。
    """
    df = pd.read_csv(io.StringIO(response), index_col=False)
    return df

def taifex_crawler(date: str) -> pd.DataFrame:
    """爬取指定日期的 TAIFEX 期貨資料。

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        處理後的期貨資料 DataFrame。
    """
    response = fetch_taifex_data(date)
    df = parse_taifex_data(response)
    df = post_process(df)
    return df

