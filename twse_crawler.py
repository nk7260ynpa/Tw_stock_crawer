

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

