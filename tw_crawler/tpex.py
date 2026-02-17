"""TPEX 上櫃股票爬蟲模組。

提供台灣櫃買中心(TPEX)每日上櫃股票資料爬取與處理功能。
"""

import logging

import cloudscraper
import pandas as pd

logger = logging.getLogger(__name__)

def webzh2en_columns() -> dict[str, str]:
    """回傳中文欄位名稱對應英文欄位名稱的字典。

    Returns:
        dict[str, str]: 中英文欄位名稱對照字典。
    """
    webzh2en_columns = {
        "代號": "Code",
        "名稱": "Name",
        "收盤 ": "Close",
        "漲跌": "Change",
        "開盤 ": "Open",
        "最高 ": "High",
        "最低": "Low",
        "成交股數  ": "TradeVolume",
        " 成交金額(元)": "TradeAmount",
        " 成交筆數 ": "NumberOfTransactions",
        "最後買價": "LastBestBidPrice",
        "最後買量<br>(千股)": "LastBidVolume",
        "最後買量<br>(張數)": "LastBidVolume",
        "最後賣價": "LastBestAskPrice",
        "最後賣量<br>(千股)": "LastBestAskVolume",
        "最後賣量<br>(張數)": "LastBestAskVolume",
        "發行股數 ": "IssuedShares",
        "次日漲停價 ": "NextDayUpLimitPrice",
        "次日跌停價": "NextDayDownLimitPrice",
    }
    return webzh2en_columns

def post_process(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """將從 TPEX 網站爬取的原始資料表做欄位轉換與清洗。

    Args:
        df: 從 TPEX 網站爬取的原始 DataFrame。
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        處理後的 DataFrame。
    """
    df = df.rename(columns=webzh2en_columns())
    df["Date"] = date
    df["Date"] = pd.to_datetime(df["Date"])
    df["Code"] = df["Code"].astype(str)
    df["Close"] = df["Close"].replace("----", None).str.replace(",", "").astype(float)
    df["Change"] = df["Change"].replace("除權", None).replace("除息", None).replace("---", None).astype(float)
    df["Open"] = df["Open"].replace("----", None).str.replace(",", "").astype(float)
    df["High"] = df["High"].replace("----", None).str.replace(",", "").astype(float)
    df["Low"] = df["Low"].replace("----", None).str.replace(",", "").astype(float)
    df["TradeVolume"] = df["TradeVolume"].str.replace(",", "").astype(int)
    df["TradeAmount"] = df["TradeAmount"].str.replace(",", "").astype(int)
    df["NumberOfTransactions"] = df["NumberOfTransactions"].str.replace(",", "").astype(int)
    df["LastBestBidPrice"] = df["LastBestBidPrice"].str.replace(",", "").astype(float)
    df["LastBidVolume"] = df["LastBidVolume"].str.replace(",", "").astype(int)
    df["LastBestAskPrice"] = df["LastBestAskPrice"].str.replace(",", "").astype(float)
    df["LastBestAskVolume"] = df["LastBestAskVolume"].str.replace(",", "").astype(int)
    df["IssuedShares"] = df["IssuedShares"].str.replace(",", "").astype(int)
    df["NextDayUpLimitPrice"] = df["NextDayUpLimitPrice"].str.replace(",", "").astype(float)
    df["NextDayDownLimitPrice"] = df["NextDayDownLimitPrice"].str.replace(",", "").astype(float)

    df = df[["Date"] + [col for col in df.columns if col != "Date"]]
    return df

def fetch_tpex_data(date: str) -> dict:
    """從 TPEX 網站取得指定日期的股票資料。

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        TPEX API 回傳的 JSON 資料。
    """
    scraper = cloudscraper.create_scraper()
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
    formatted_date = date.replace("-", "/")
    data = {"date": formatted_date, "type": "AL"}
    response = scraper.post(url, data=data).json()
    return response

def parse_tpex_data(response: dict) -> pd.DataFrame:
    """將 TPEX API 回傳的 JSON 解析為 DataFrame。

    Args:
        response: TPEX API 回傳的 JSON 資料。

    Returns:
        解析後的 DataFrame。
    """
    fields = response["tables"][0]["fields"]
    data = response["tables"][0]["data"]
    df = pd.DataFrame(columns=fields, data=data)
    return df

def tpex_crawler(date: str) -> pd.DataFrame:
    """爬取指定日期的 TPEX 上櫃股票資料。

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        處理後的股票資料 DataFrame。
    """
    logger.info(f"Starting Request data from TPEX")
    response = fetch_tpex_data(date)
    df = parse_tpex_data(response)
    df = post_process(df, date)
    return df


    
