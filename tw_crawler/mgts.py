"""MGTS 融資融券爬蟲模組。

提供台灣融資融券每日資料爬取與處理功能。
"""

import requests
import logging

import pandas as pd

logger = logging.getLogger(__name__)

def en_columns():
    """
    Return English columns for MGTS crawler

    Returns:
        list: English columns for MGTS crawler

    Examples:
        >>> en_columns()
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
    """
    回傳一個中文欄位名稱對應到英文欄位名稱的字典

    Returns:
        dict: 中文欄位名稱對應到英文欄位名稱的字典
    
    Examples:
        >>> zh2en_columns()
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

def remove_comma(x):
    """
    Remove comma from a string.

    Args:
        x (str): a string with commas

    Returns:
        str: the string with commas removed

    Examples:
        >>> remove_comma("1,234")
    """
    return x.replace(",", "")

def post_process(df, date):
    df.columns = en_columns()
    df["Date"] = date
    df["Date"] = pd.to_datetime(df["Date"])
    df["MarginPurchase"] = df["MarginPurchase"].map(remove_comma).astype(int)
    df["MarginSales"] = df["MarginSales"].map(remove_comma).astype(int)
    df["CashRedemption"] = df["CashRedemption"].map(remove_comma).astype(int)
    df["MarginPurchaseBalanceOfPreviousDay"] = df["MarginPurchaseBalanceOfPreviousDay"].map(remove_comma).astype(int)
    df["MarginPurchaseBalanceOfTheDay"] = df["MarginPurchaseBalanceOfTheDay"].map(remove_comma).astype(int)
    df["MarginPurchaseQuotaForTheNextDay"] = df["MarginPurchaseQuotaForTheNextDay"].map(remove_comma).astype(int)
    df["ShortCovering"] = df["ShortCovering"].map(remove_comma).astype(int)
    df["ShortSale"] = df["ShortSale"].map(remove_comma).astype(int)
    df["StockRedemption"] = df["StockRedemption"].map(remove_comma).astype(int)
    df["ShortSaleBalanceOfPreviousDay"] = df["ShortSaleBalanceOfPreviousDay"].map(remove_comma).astype(int)
    df["ShortSaleBalanceOfTheDay"] = df["ShortSaleBalanceOfTheDay"].map(remove_comma).astype(int)
    df["ShortSaleQuotaForTheNextDay"] = df["ShortSaleQuotaForTheNextDay"].map(remove_comma).astype(int)
    df["OffsettingOfMarginPurchasesAndShortSales"] = df["OffsettingOfMarginPurchasesAndShortSales"].map(remove_comma).astype(int)
    df["Note"] = df["Note"].astype(str)

    df = df[["Date"] + [col for col in df.columns if col != "Date"]]
    return df

def gen_empty_date_df():
    """
    generate an empty DataFrame when MGTS is not open
    
    Returns:
        pd.DataFrame: an empty DataFrame with the correct columns
    """
    df = pd.DataFrame(columns=en_columns())
    df.insert(0, "Date", pd.NaT)
    return df

def parse_mgts_data(response, date):
    """
    Parse the JSON response from the MGTS website into a DataFrame.

    Args:
        data (dict): The JSON response from the MGTS website.

    Returns:
        pd.DataFrame: The parsed DataFrame.

    Examples:
        >>> parse_mgts_data(data)
    """
    if response["stat"] == "OK":
        df = pd.DataFrame(columns=response["tables"][1]["fields"], data=response["tables"][1]["data"])
        df = post_process(df, date)
    else:
        df = gen_empty_date_df()
    return df

def fetch_mgts_data(date):
    """
    Crawl the MGTS website for stock data on a given date and process it.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: The processed DataFrame containing stock data.

    Examples:
        >>> MGTS_crawler("2022-02-18")
    """
    url = f'https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={date.replace("-", "")}&selectType=ALL&response=json'
    response = requests.get(url)
    return response.json()

def mgts_crawler(date):
    """
    Crawl the MGTS website for stock data on a given date and process it.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: The processed DataFrame containing stock data.

    Examples:
        >>> mgts_crawler("2022-02-18")
    """
    logger.info(f"Starting Request data from Foreign and Other Investors")
    response = fetch_mgts_data(date)
    df = parse_mgts_data(response, date)
    return df