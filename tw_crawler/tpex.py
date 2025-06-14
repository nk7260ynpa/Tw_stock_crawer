import logging

import cloudscraper
import pandas as pd

logger = logging.getLogger(__name__)

def webzh2en_columns() -> dict[str, str]:
    """
    回傳一個中文欄位名稱對應到英文欄位名稱的字典

    Returns:
        dict: 中文欄位名稱對應到英文欄位名稱的字典
    
    Examples:
        >>> zh2en_columns()
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

def post_process(df, date) -> pd.DataFrame:
    """
    將從tpex網站爬下來的資料表做專門的處理

    Args:
        df (pd.DataFrame): 剛從tpex網站爬下來的資料表

    Returns:
        pd.DataFrame: 根據每個column做完各自處理的資料表

    Examples:
        >>> df = post_process(df)
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
    """
    Fetch data from the TPEX website for a given date.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        dict: The JSON response from the TPEX website.
    """
    scraper = cloudscraper.create_scraper()
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
    formatted_date = date.replace("-", "/")
    data = {"date": formatted_date, "type": "AL"}
    response = scraper.post(url, data=data).json()
    return response

def parse_tpex_data(response: dict) -> pd.DataFrame:
    """
    Parse the JSON response from the TPEX website into a DataFrame.

    Args:
        response (dict): The JSON response from the TPEX website.

    Returns:
        pd.DataFrame: The parsed DataFrame.
    """
    fields = response["tables"][0]["fields"]
    data = response["tables"][0]["data"]
    df = pd.DataFrame(columns=fields, data=data)
    return df

def tpex_crawler(date: str) -> pd.DataFrame:
    """
    Crawl the TPEX website for stock data on a given date and process it.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: The processed DataFrame containing stock data.
    """
    logger.info(f"Starting Request data from TPEX")
    response = fetch_tpex_data(date)
    df = parse_tpex_data(response)
    df = post_process(df, date)
    return df


    
