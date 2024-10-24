
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