import cloudscraper
import pandas as pd

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
        "成交股數  ": "TradeVol(shares)",
        " 成交金額(元)": "TradeAmt.(NTD)",
        " 成交筆數 ": "No.ofTransactions",
        "最後買價": "LastBestBidPrice",
        "最後買量<br>(千股)": "LastBidVolume",
        "最後賣價": "LastBestAskPrice",
        "最後賣量<br>(千股)": "LastBestAskVolume",
        "發行股數 ": "IssuedShares",
        "次日漲停價 ": "NextDayUpLimitPrice",
        "次日跌停價": "NextDayDownLimitPrice",
    }
    return webzh2en_columns

def post_process(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=webzh2en_columns())
    df["Code"] = df["Code"].astype(str)
    df["Close"] = df["Close"].replace("----", None).str.replace(",", "").astype(float)
    df["Change"] = df["Change"].replace("---", None).astype(float)
    df["Open"] = df["Open"].replace("----", None).str.replace(",", "").astype(float)
    df["High"] = df["High"].replace("----", None).str.replace(",", "").astype(float)
    df["Low"] = df["Low"].replace("----", None).str.replace(",", "").astype(float)
    df["TradeVol(shares)"] = df["TradeVol(shares)"].str.replace(",", "").astype(float)
    df["TradeAmt.(NTD)"] = df["TradeAmt.(NTD)"].str.replace(",", "").astype(float)
    df["No.ofTransactions"] = df["No.ofTransactions"].str.replace(",", "").astype(int)
    df["LastBestBidPrice"] = df["LastBestBidPrice"].str.replace(",", "").astype(float)
    df["LastBidVolume"] = df["LastBidVolume"].str.replace(",", "").astype(float)
    df["LastBestAskPrice"] = df["LastBestAskPrice"].str.replace(",", "").astype(float)
    df["LastBestAskVolume"] = df["LastBestAskVolume"].str.replace(",", "").astype(float)
    df["IssuedShares"] = df["IssuedShares"].str.replace(",", "").astype(float)
    df["NextDayUpLimitPrice"] = df["NextDayUpLimitPrice"].str.replace(",", "").astype(float)
    df["NextDayDownLimitPrice"] = df["NextDayDownLimitPrice"].str.replace(",", "").astype(float)
    return df

def tpex_crawler(date):
    import cloudscraper
    scraper = cloudscraper.create_scraper()
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
    data = {"date": "2024/10/25", "type": "AL"}
    response = scraper.post(url, data=data)
    response = response.json()
    df = pd.DataFrame(columns=response["tables"][0]["fields"], data=response["tables"][0]["data"])
    df = post_process(df)

    return df


    
