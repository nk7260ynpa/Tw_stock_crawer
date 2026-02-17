"""FAOI 三大法人爬蟲模組。

提供台灣三大法人(外資、投信、自營商)每日買賣超資料爬取與處理功能。
"""

import logging

import pandas as pd
import requests

logger = logging.getLogger(__name__)

def en_columns() -> list[str]:
    """回傳 FAOI 爬蟲的英文欄位名稱列表。

    Returns:
        list[str]: FAOI 英文欄位名稱列表。
    """
    en_columns = [
        "SecurityCode",
        "StockName",
        "ForeignInvestorsTotalBuy",
        "ForeignInvestorsTotalSell",
        "ForeignInvestorsDifference",
        "ForeignDealersTotalBuy",
        "ForeignDealersTotalSell",
        "ForeignDealersDifference",
        "SecuritiesInvestmentTotalBuy",
        "SecuritiesInvestmentTotalSell",
        "SecuritiesInvestmentDifference",
        "DealersDifference",
        "DealersProprietaryTotalBuy",
        "DealersProprietaryTotalSell",
        "DealersProprietaryDifference",
        "DealersHedgeTotalBuy",
        "DealersHedgeTotalSell",
        "DealersHedgeDifference",
        "TotalDifference"
    ]
    return en_columns

def zh2en_columns() -> dict[str, str]:
    """回傳中文欄位名稱對應英文欄位名稱的字典。

    Returns:
        dict[str, str]: 中英文欄位名稱對照字典。
    """
    zh2en_columns = {
        "證券代號": "SecurityCode",
        "證券名稱": "StockName",
        "外陸資買進股數(不含外資自營商)": "ForeignInvestorsTotalBuy",
        "外陸資賣出股數(不含外資自營商)": "ForeignInvestorsTotalSell",
        "外陸資買賣超股數(不含外資自營商)": "ForeignInvestorsDifference",
        "外資自營商買進股數": "ForeignDealersTotalBuy",
        "外資自營商賣出股數": "ForeignDealersTotalSell",
        "外資自營商買賣超股數": "ForeignDealersDifference",
        "投信買進股數": "SecuritiesInvestmentTotalBuy",
        "投信賣出股數": "SecuritiesInvestmentTotalSell",
        "投信買賣超股數": "SecuritiesInvestmentDifference",
        "自營商買賣超股數": "DealersDifference",
        "自營商買進股數(自行買賣)": "DealersProprietaryTotalBuy",
        "自營商賣出股數(自行買賣)": "DealersProprietaryTotalSell",
        "自營商買賣超股數(自行買賣)": "DealersProprietaryDifference",
        "自營商買進股數(避險)": "DealersHedgeTotalBuy",
        "自營商賣出股數(避險)": "DealersHedgeTotalSell",
        "自營商買賣超股數(避險)": "DealersHedgeDifference",
        "三大法人買賣超股數": "TotalDifference"
    }
    return zh2en_columns

def remove_comma(x: str) -> str:
    """移除字串中的逗號。

    Args:
        x: 含有逗號的字串。

    Returns:
        移除逗號後的字串。
    """
    return x.replace(",", "")

def post_process(df: pd.DataFrame, date: str) -> pd.DataFrame:
    df = df.rename(columns=zh2en_columns())
    df["Date"] = date
    df["Date"] = pd.to_datetime(df["Date"])
    df["ForeignInvestorsTotalBuy"] = df["ForeignInvestorsTotalBuy"].map(remove_comma).astype(int)
    df["ForeignInvestorsTotalSell"] = df["ForeignInvestorsTotalSell"].map(remove_comma).astype(int)
    df["ForeignInvestorsDifference"] = df["ForeignInvestorsDifference"].map(remove_comma).astype(int)
    df["ForeignDealersTotalBuy"] = df["ForeignDealersTotalBuy"].astype(str).map(remove_comma).astype(int)
    df["ForeignDealersTotalSell"] = df["ForeignDealersTotalSell"].astype(str).map(remove_comma).astype(int)
    df["ForeignDealersDifference"] = df["ForeignDealersDifference"].astype(str).map(remove_comma).astype(int)
    df["SecuritiesInvestmentTotalBuy"] = df["SecuritiesInvestmentTotalBuy"].map(remove_comma).astype(int)
    df["SecuritiesInvestmentTotalSell"] = df["SecuritiesInvestmentTotalSell"].map(remove_comma).astype(int)
    df["SecuritiesInvestmentDifference"] = df["SecuritiesInvestmentDifference"].map(remove_comma).astype(int)
    df["DealersDifference"] = df["DealersDifference"].map(remove_comma).astype(int)
    df["DealersProprietaryTotalBuy"] = df["DealersProprietaryTotalBuy"].map(remove_comma).astype(int)
    df["DealersProprietaryTotalSell"] = df["DealersProprietaryTotalSell"].map(remove_comma).astype(int)
    df["DealersProprietaryDifference"] = df["DealersProprietaryDifference"].map(remove_comma).astype(int)
    df["DealersHedgeTotalBuy"] = df["DealersHedgeTotalBuy"].astype(object).fillna("0").astype(str).map(remove_comma).astype(float).astype(int)
    df["DealersHedgeTotalSell"] = df["DealersHedgeTotalSell"].astype(object).fillna("0").astype(str).map(remove_comma).astype(float).astype(int)
    df["DealersHedgeDifference"] = df["DealersHedgeDifference"].astype(object).fillna("0").astype(str).map(remove_comma).astype(float).astype(int)
    df["TotalDifference"] = df["TotalDifference"].map(remove_comma).astype(int)

    df = df[["Date"] + [col for col in df.columns if col != "Date"]]
    return df

def gen_empty_date_df() -> pd.DataFrame:
    """產生 FAOI 休市時的空 DataFrame。

    Returns:
        具有正確欄位的空 DataFrame。
    """
    df = pd.DataFrame(columns=en_columns())
    df.insert(0, "Date", pd.NaT)
    return df

def parse_faoi_data(response: dict, date: str) -> pd.DataFrame:
    """將 FAOI API 回傳的 JSON 解析為 DataFrame。

    Args:
        response: FAOI API 回傳的 JSON 資料。
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        解析並處理後的 DataFrame。
    """
    if response["stat"] == "OK":
        df = pd.DataFrame(columns=response["fields"], data=response["data"])
        df = post_process(df, date)
    else:
        df = gen_empty_date_df()
    return df

def fetch_faoi_data(date: str) -> dict:
    """從 TWSE 網站取得指定日期的三大法人買賣超資料。

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        FAOI API 回傳的 JSON 資料。
    """
    url = f'https://www.twse.com.tw/rwd/zh/fund/T86?date={date.replace("-", "")}&selectType=ALL&response=json'
    response = requests.get(url)
    return response.json()

def faoi_crawler(date: str) -> pd.DataFrame:
    """爬取指定日期的三大法人買賣超資料。

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        處理後的三大法人資料 DataFrame。
    """
    logger.info(f"Starting Request data from Foreign and Other Investors")
    response = fetch_faoi_data(date)
    df = parse_faoi_data(response, date)
    return df