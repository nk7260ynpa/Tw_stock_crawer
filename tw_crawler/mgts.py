"""MGTS 融資融券爬蟲模組。

提供台灣融資融券每日資料爬取與處理功能。
"""

import logging

import pandas as pd
import requests

logger = logging.getLogger(__name__)


def en_columns() -> list[str]:
    """回傳 MGTS 爬蟲的英文欄位名稱列表。

    Returns:
        list[str]: MGTS 英文欄位名稱列表。
    """
    en_columns = [
        "SecurityCode",
        "StockName",
        "MarginPurchase",
        "MarginSales",
        "CashRedemption",
        "MarginPurchaseBalanceOfPreviousDay",
        "MarginPurchaseBalanceOfTheDay",
        "MarginPurchaseQuotaForTheNextDay",
        "ShortCovering",
        "ShortSale",
        "StockRedemption",
        "ShortSaleBalanceOfPreviousDay",
        "ShortSaleBalanceOfTheDay",
        "ShortSaleQuotaForTheNextDay",
        "OffsettingOfMarginPurchasesAndShortSales",
        "Note"
    ]
    return en_columns


def zh2en_columns() -> dict[str, str]:
    """回傳中文欄位名稱對應英文欄位名稱的字典。

    Returns:
        dict[str, str]: 中英文欄位名稱對照字典。
    """
    zh2en_columns = {
        "日期": "Date",
        "代號": "SecurityCode",
        "名稱": "StockName",
        "融資買進": "MarginPurchase",
        "融資賣出": "MarginSales",
        "融資現金償還": "CashRedemption",
        "融資前日餘額": "MarginPurchaseBalanceOfPreviousDay",
        "融資當日餘額": "MarginPurchaseBalanceOfTheDay",
        "融資隔日限額": "MarginPurchaseQuotaForTheNextDay",
        "融券買進": "ShortCovering",
        "融券賣出": "ShortSale",
        "融券現券償還": "StockRedemption",
        "融券前日餘額": "ShortSaleBalanceOfPreviousDay",
        "融券當日餘額": "ShortSaleBalanceOfTheDay",
        "融券隔日限額": "ShortSaleQuotaForTheNextDay",
        "資券互抵": "OffsettingOfMarginPurchasesAndShortSales",
        "註記": "Note"
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
    """將從 MGTS 網站爬取的原始資料表做欄位轉換與清洗。

    Args:
        df: 從 MGTS 網站爬取的原始 DataFrame。
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        處理後的 DataFrame。
    """
    df.columns = en_columns()
    df["Date"] = date
    df["Date"] = pd.to_datetime(df["Date"])
    df["MarginPurchase"] = (
        df["MarginPurchase"].map(remove_comma).astype(int)
    )
    df["MarginSales"] = (
        df["MarginSales"].map(remove_comma).astype(int)
    )
    df["CashRedemption"] = (
        df["CashRedemption"].map(remove_comma).astype(int)
    )
    df["MarginPurchaseBalanceOfPreviousDay"] = (
        df["MarginPurchaseBalanceOfPreviousDay"]
        .map(remove_comma).astype(int)
    )
    df["MarginPurchaseBalanceOfTheDay"] = (
        df["MarginPurchaseBalanceOfTheDay"]
        .map(remove_comma).astype(int)
    )
    df["MarginPurchaseQuotaForTheNextDay"] = (
        df["MarginPurchaseQuotaForTheNextDay"]
        .map(remove_comma).astype(int)
    )
    df["ShortCovering"] = (
        df["ShortCovering"].map(remove_comma).astype(int)
    )
    df["ShortSale"] = (
        df["ShortSale"].map(remove_comma).astype(int)
    )
    df["StockRedemption"] = (
        df["StockRedemption"].map(remove_comma).astype(int)
    )
    df["ShortSaleBalanceOfPreviousDay"] = (
        df["ShortSaleBalanceOfPreviousDay"]
        .map(remove_comma).astype(int)
    )
    df["ShortSaleBalanceOfTheDay"] = (
        df["ShortSaleBalanceOfTheDay"]
        .map(remove_comma).astype(int)
    )
    df["ShortSaleQuotaForTheNextDay"] = (
        df["ShortSaleQuotaForTheNextDay"]
        .map(remove_comma).astype(int)
    )
    df["OffsettingOfMarginPurchasesAndShortSales"] = (
        df["OffsettingOfMarginPurchasesAndShortSales"]
        .map(remove_comma).astype(int)
    )
    df["Note"] = df["Note"].astype(str)

    df = df[["Date"] + [col for col in df.columns if col != "Date"]]
    return df


def gen_empty_date_df() -> pd.DataFrame:
    """產生 MGTS 休市時的空 DataFrame。

    Returns:
        具有正確欄位的空 DataFrame。
    """
    df = pd.DataFrame(columns=en_columns())
    df.insert(0, "Date", pd.NaT)
    return df


def parse_mgts_data(response: dict, date: str) -> pd.DataFrame:
    """將 MGTS API 回傳的 JSON 解析為 DataFrame。

    Args:
        response: MGTS API 回傳的 JSON 資料。
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        解析並處理後的 DataFrame。
    """
    if response["stat"] == "OK":
        df = pd.DataFrame(
            columns=response["tables"][1]["fields"],
            data=response["tables"][1]["data"],
        )
        df = post_process(df, date)
    else:
        df = gen_empty_date_df()
    return df


def fetch_mgts_data(date: str) -> dict:
    """從 TWSE 網站取得指定日期的融資融券資料。

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        MGTS API 回傳的 JSON 資料。
    """
    url = (
        "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
        f"?date={date.replace('-', '')}&selectType=ALL&response=json"
    )
    response = requests.get(url)
    return response.json()


def mgts_crawler(date: str) -> pd.DataFrame:
    """爬取指定日期的融資融券資料。

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        處理後的融資融券資料 DataFrame。
    """
    logger.info("Starting Request data from MGTS")
    response = fetch_mgts_data(date)
    df = parse_mgts_data(response, date)
    return df
