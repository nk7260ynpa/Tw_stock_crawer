import requests
import pandas as pd

def twse_headers():
    """
    Return headers for TWSE crawler

    Returns:
        dict: headers for TWSE crawler

    Examples:
        >>> twse_headers()
    """
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7',
        'Connection': 'keep-alive',
        'Host': 'www.twse.com.tw',
        'Referer': 'https://www.twse.com.tw/zh/trading/historical/mi-index.html',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
         }
    return headers

def en_columns():
    """
    Return English columns for TWSE crawler

    Returns:
        list: English columns for TWSE crawler

    Examples:
        >>> en_columns()
    """
    en_columns = [
        "StockID",
        "StockName",
        "TradeVolume",
        "Transaction",
        "TradeValue",
        "OpenPrice",
        "HightestPrice",
        "LowestPrice",
        "ClosePrice",
        "PriceChangeSign",
        "PriceChange",
        "FinalBuyPrice",
        "FinalBuyVolume",
        "FinalSellPrice",
        "FinalSellVolume",
        "PER"
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
        "證券代號": "StockID",
        "證券名稱": "StockName",
        "成交股數": "TradeVolume",
        "成交筆數": "Transaction",
        "成交金額": "TradeValue",
        "開盤價": "OpenPrice",
        "最高價": "HightestPrice",
        "最低價": "LowestPrice",
        "收盤價": "ClosePrice",
        "漲跌(+/-)": "PriceChangeSign",
        "漲跌價差": "PriceChange",
        "最後揭示買價": "FinalBuyPrice",
        "最後揭示買量": "FinalBuyVolume",
        "最後揭示賣價": "FinalSellPrice",
        "最後揭示賣量": "FinalSellVolume",
        "本益比": "PER"
    }
    return zh2en_columns

def html2signal() -> dict:
    html2signal = {
        "<p> </p>": " ",
        "<p style= color:green>-</p>": "-",
        "<p style= color:red>+</p>": "+",
        "<p>X</p>": "X"
    }
    return html2signal

def remove_comma(x: str) -> str:
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

def post_process(df) -> pd.DataFrame:
    df = df.rename(columns=zh2en_columns())
    df["PriceChangeSign"] = df["PriceChangeSign"].map(html2signal())
    df["TradeVolume"] = df["TradeVolume"].map(remove_comma).astype(int)
    df["Transaction"] = df["Transaction"].map(remove_comma).astype(int)
    df["TradeValue"] = df["TradeValue"].map(remove_comma).astype(int)
    df["OpenPrice"] = df["OpenPrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["HightestPrice"] = df["HightestPrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["LowestPrice"] = df["LowestPrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["ClosePrice"] = df["ClosePrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["PriceChange"] = df["PriceChange"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["FinalBuyPrice"] = df["FinalBuyPrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["FinalBuyVolume"] = df["FinalBuyVolume"].str.replace(",", "").str.replace("--", "0").str.replace("", "0").astype(int)
    df["FinalSellPrice"] = df["FinalSellPrice"].str.replace(",", "").str.replace("--", "0").astype(float)
    df["FinalSellVolume"] = df["FinalSellVolume"].str.replace(",", "").str.replace("--", "0").str.replace("", "0").astype(int)
    df["PER"] = df["PER"].str.replace(",", "").astype(float)
    return df

def fetch_twse_data(date: str) -> dict:
    """
    Fetch data from the TWSE website for a given date.

    Args:
        date (str): the date of the data to be fetched

    Returns:
        dict: the fetched data

    Examples:
        >>> fetch_twse_data("2022-02-18")
    """
    url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date.replace("-", "")}&type=ALL&response=json'
    response = requests.get(url, headers=twse_headers())
    return response.json()

def parse_twse_data(response) -> pd.DataFrame:
    """
    Parse the JSON response from the TWSE website into a DataFrame.

    Args:
        data (dict): The JSON response from the TWSE website.

    Returns:
        pd.DataFrame: The parsed DataFrame.

    Examples:
        >>> parse_twse_data(data)
    """

    if response["stat"] == "OK":
        target_table = response["tables"][8]
        df = pd.DataFrame(columns=target_table["fields"], data=target_table["data"])
        df = post_process(df)
    else:
        df = pd.DataFrame(columns=en_columns())
    return df

def twse_crawler(date: str) -> pd.DataFrame:
    """
    Crawl the TWSE website for stock data on a given date and process it.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: The processed DataFrame containing stock data.

    Examples:
        >>> twse_crawler("2022-02-18")
    """
    response = fetch_twse_data(date)
    df = parse_twse_data(response)
    return df