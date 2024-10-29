import cloudscraper
import numpy as np
import pandas as pd
import io

def webzh2en_columns():
    """
    回傳一個中文欄位名稱對應到英文欄位名稱的字典

    Returns:
        dict: 中文欄位名稱對應到英文欄位名稱的字典
    
    Examples:
        >>> webzh2en_columns()
    """
    webzh2en_columns = {
        "交易日期": "Date",
        "契約": "Contract",
        "到期月份(週別)": "ContractMonth(Week)",
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

def post_process(df):
    """
    將從taifex網站爬下來的資料表做專門的處理

    Args:
        df (pd.DataFrame): 剛從taifex網站爬下來的資料表

    Returns:
        pd.DataFrame: 根據每個column做完各自處理的資料表

    Examples:
        >>> df = post_process(df)
    """
    df = df.rename(columns=webzh2en_columns())
    df["Date"] = pd.to_datetime(df["Date"], format="%Y/%m/%d")
    df["Contract"] = df["Contract"].astype(str)
    df["ContractMonth(Week)"] = df["ContractMonth(Week)"].astype(str)
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
    df["TradingHalt"] = df["TradingHalt"].replace("-", None).replace(" ", "").replace("是", True).replace("否", False)
    df["TradingSession"] = df["TradingSession"].astype(str)
    df["SpreadOrderVolume"] = df["SpreadOrderVolume"].astype(float)
    return df

def fetch_taifex_data(date: str) -> pd.DataFrame:
    """
    Fetch data from Taifex website for a given date.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: The data table fetched from the Taifex website.

    Examples:
        >>> df = fetch_taifex_data("2024-10-29")
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
    """
    Parse the data table from the response text.

    Args:
        response (str): The response text from the Taifex website.
        
        Returns:
        pd.DataFrame: The data table parsed from the response text.
        
        Examples:
        >>> df = parse_taifex_data(response)
    """
    df = pd.read_csv(io.StringIO(response), index_col=False)
    return df

def taifex_crawler(date: str) -> pd.DataFrame:
    """
    Crawl the Taifex website for data on a given date.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: The processed DataFrame containing stock data.

    Examples:
        >>> df = taifex_crawler("2024-10-29")
    """
    response = fetch_taifex_data(date)
    df = parse_taifex_data(response)
    df = post_process(df)
    return df

if __name__ == "__main__":
    df = taifex_crawler("2024-10-29")