import requests
import logging

import pandas as pd

logger = logging.getLogger(__name__)

def en_columns():
    en_columns = [
        "SecurityCode",
        "ForeignInvestorsTotalBuy",
        "ForeignInvestorsTotalSell",
        "ForeignInvestorsDifference",
        "ForeignDealersTotalBuy",
        "ForeignDealersTotalSell",
        "ForeignDealersDifference",
        "SecuritiesInvestmentTotalBuy",
        "SecuritiesInvestmentTotalSell",
        "SecuritiesInvestmentDifference",
        "Dealers Difference",
        "DealersProprietaryTotalBuy",
        "DealersProprietaryTotalSell",
        "DealersProprietaryDifference",
        "DealersHedgeTotalBuy",
        "DealersHedgeTotalSell",
        "DealersHedgeDifference",
        "Total Difference"
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

def post_process(df, date) -> pd.DataFrame:
    df = df.rename(columns=zh2en_columns())
    df["Date"] = date
    df["Date"] = pd.to_datetime(df["Date"])
    df["TradeVolume"] = df["TradeVolume"].map(remove_comma).astype(int)
    df["Transaction"] = df["Transaction"].map(remove_comma).astype(int)
    df["TradeValue"] = df["TradeValue"].map(remove_comma).astype(int)
    df["OpeningPrice"] = df["OpeningPrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["HighestPrice"] = df["HighestPrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["LowestPrice"] = df["LowestPrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["ClosingPrice"] = df["ClosingPrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["Dir"] = df["Dir"].map(html2signal()).astype(float)
    df["Change"] = df["Change"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["Change"] = df["Change"] * df["Dir"]
    df["LastBestBidPrice"] = df["LastBestBidPrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["LastBestBidVolume"] = df["LastBestBidVolume"].map(lambda x: {"": "0"}.get(x, x)).str.replace(",", "").str.replace("--", "0").astype(int)
    df["LastBestAskPrice"] = df["LastBestAskPrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["LastBestAskVolume"] = df["LastBestAskVolume"].map(lambda x: {"": "0"}.get(x, x)).str.replace(",", "").str.replace("--", "0").astype(int)
    df["PriceEarningratio"] = df["PriceEarningratio"].str.replace(",", "").astype(float)

    df = df.drop(columns=["Dir"])
    df = df[["Date"] + [col for col in df.columns if col != "Date"]]
    return df

def gen_empty_date_df():
    """
    generate an empty DataFrame when TWSE is not open
    
    Returns:
        pd.DataFrame: an empty DataFrame with the correct columns
    """
    df = pd.DataFrame(columns=en_columns())
    df.insert(0, "Date", pd.NaT)
    df = df.drop(columns=["Dir"])
    return df

def parse_faoi_data(response, date):
    if response["stat"] == "OK":
        df = pd.DataFrame(columns=response["fields"], data=response["data"])
        df = post_process(df, date)
    else:
        df = gen_empty_date_df()
    return df

def fetch_faoi_data(date):
    url = f'https://www.twse.com.tw/rwd/zh/fund/T86?date={date.replace("-", "")}&selectType=ALL&response=json'
    response = requests.get(url)
    return response.json()

def faoi_crawler(date):
    logger.info(f"Starting Request data from Foreign and Other Investors")
    response = fetch_faoi_data(date)
    df = parse_faoi_data(response, date)
    return df