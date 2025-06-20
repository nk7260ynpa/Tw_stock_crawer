import requests
import logging

import pandas as pd

logger = logging.getLogger(__name__)

def en_columns():
    """
    Return English columns for FAOI crawler

    Returns:
        list: English columns for FAOI crawler

    Examples:
        >>> en_columns()
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
    """
    回傳一個中文欄位名稱對應到英文欄位名稱的字典

    Returns:
        dict: 中文欄位名稱對應到英文欄位名稱的字典
    
    Examples:
        >>> zh2en_columns()
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
        "自營商買賣超股數": "Dealers Difference",
        "自營商買進股數(自行買賣)": "DealersProprietaryTotalBuy",
        "自營商賣出股數(自行買賣)": "DealersProprietaryTotalSell",
        "自營商買賣超股數(自行買賣)": "DealersProprietaryDifference",
        "自營商買進股數(避險)": "DealersHedgeTotalBuy",
        "自營商賣出股數(避險)": "DealersHedgeTotalSell",
        "自營商買賣超股數(避險)": "DealersHedgeDifference",
        "三大法人買賣超股數": "Total Difference"
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

def post_process(df, date) -> pd.DataFrame:
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
    df["Dealers Difference"] = df["Dealers Difference"].map(remove_comma).astype(int)
    df["DealersProprietaryTotalBuy"] = df["DealersProprietaryTotalBuy"].map(remove_comma).astype(int)
    df["DealersProprietaryTotalSell"] = df["DealersProprietaryTotalSell"].map(remove_comma).astype(int)
    df["DealersProprietaryDifference"] = df["DealersProprietaryDifference"].map(remove_comma).astype(int)
    df["DealersHedgeTotalBuy"] = df["DealersHedgeTotalBuy"].fillna(0).map(remove_comma).astype(int)
    df["DealersHedgeTotalSell"] = df["DealersHedgeTotalSell"].fillna(0).map(remove_comma).astype(int)
    df["DealersHedgeDifference"] = df["DealersHedgeDifference"].fillna(0).map(remove_comma).astype(int)
    df["Total Difference"] = df["Total Difference"].map(remove_comma).astype(int)

    df = df[["Date"] + [col for col in df.columns if col != "Date"]]
    return df

def gen_empty_date_df():
    """
    generate an empty DataFrame when FAOI is not open
    
    Returns:
        pd.DataFrame: an empty DataFrame with the correct columns
    """
    df = pd.DataFrame(columns=en_columns())
    df.insert(0, "Date", pd.NaT)
    return df

def parse_faoi_data(response, date):
    """
    Parse the JSON response from the FAOI website into a DataFrame.

    Args:
        data (dict): The JSON response from the FAOI website.

    Returns:
        pd.DataFrame: The parsed DataFrame.

    Examples:
        >>> parse_faoi_data(data)
    """
    if response["stat"] == "OK":
        df = pd.DataFrame(columns=response["fields"], data=response["data"])
        df = post_process(df, date)
    else:
        df = gen_empty_date_df()
    return df

def fetch_faoi_data(date):
    """
    Crawl the FAOI website for stock data on a given date and process it.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: The processed DataFrame containing stock data.

    Examples:
        >>> faoi_crawler("2022-02-18")
    """
    url = f'https://www.twse.com.tw/rwd/zh/fund/T86?date={date.replace("-", "")}&selectType=ALL&response=json'
    response = requests.get(url)
    return response.json()

def faoi_crawler(date):
    """
    Crawl the FAOI website for stock data on a given date and process it.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: The processed DataFrame containing stock data.

    Examples:
        >>> faoi_crawler("2022-02-18")
    """
    logger.info(f"Starting Request data from Foreign and Other Investors")
    response = fetch_faoi_data(date)
    df = parse_faoi_data(response, date)
    return df