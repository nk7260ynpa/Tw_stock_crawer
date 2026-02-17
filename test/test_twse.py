"""TWSE 上市股票爬蟲測試模組。"""

import pandas as pd
import tw_crawler.twse as twse

def test_en_columns():
    result = twse.en_columns()
    expect = [
        "SecurityCode",
        "StockName",
        "TradeVolume",
        "Transaction",
        "TradeValue",
        "OpeningPrice",
        "HighestPrice",
        "LowestPrice",
        "ClosingPrice",
        "Dir",
        "Change",
        "LastBestBidPrice",
        "LastBestBidVolume",
        "LastBestAskPrice",
        "LastBestAskVolume",
        "PriceEarningratio"
    ]
    assert result == expect

def test_zh2en_columns():
    result = twse.zh2en_columns()
    expect = {
        "證券代號": "SecurityCode",
        "證券名稱": "StockName",
        "成交股數": "TradeVolume",
        "成交筆數": "Transaction",
        "成交金額": "TradeValue",
        "開盤價": "OpeningPrice",
        "最高價": "HighestPrice",
        "最低價": "LowestPrice",
        "收盤價": "ClosingPrice",
        "漲跌(+/-)": "Dir",
        "漲跌價差": "Change",
        "最後揭示買價": "LastBestBidPrice",
        "最後揭示買量": "LastBestBidVolume",
        "最後揭示賣價": "LastBestAskPrice",
        "最後揭示賣量": "LastBestAskVolume",
        "本益比": "PriceEarningratio"
    }
    assert result == expect

def test_html2signal():
    result = twse.html2signal()
    expect = {
        "<p> </p>": 0,
        "<p style= color:green>-</p>": -1,
        "<p style= color:red>+</p>": 1,
        "<p>X</p>": 0
    }
    assert result == expect

def test_remove_comma():
    result = twse.remove_comma("1,234,567")
    expect = "1234567"
    assert result == expect

def test_post_process():
    data = {
        "證券代號": ["2330"],
        "證券名稱": ["台積電"],
        "成交股數": ["1,234,567"],
        "成交筆數": ["1,234"],
        "成交金額": ["123,456,789"],
        "開盤價": ["600.0"],
        "最高價": ["610.0"],
        "最低價": ["590.0"],
        "收盤價": ["605.0"],
        "漲跌(+/-)": ["<p style= color:red>+</p>"],
        "漲跌價差": ["5.0"],
        "最後揭示買價": ["604.0"],
        "最後揭示買量": ["1,000"],
        "最後揭示賣價": ["605.1"],
        "最後揭示賣量": ["2,000"],
        "本益比": ["20.0"]
    }
    df = pd.DataFrame(data)
    date = "2022-02-18"
    result = twse.post_process(df, date)
    expect = pd.DataFrame({
        "Date": pd.to_datetime(["2022-02-18"]),
        "SecurityCode": ["2330"],
        "StockName": ["台積電"],
        "TradeVolume": [1234567],
        "Transaction": [1234],
        "TradeValue": [123456789],
        "OpeningPrice": [600.0],
        "HighestPrice": [610.0],
        "LowestPrice": [590.0],
        "ClosingPrice": [605.0],
        "Change": [5.0],
        "LastBestBidPrice": [604.0],
        "LastBestBidVolume": [1000],
        "LastBestAskPrice": [605.1],
        "LastBestAskVolume": [2000],
        "PriceEarningratio": [20.0]
    })
    pd.testing.assert_frame_equal(result, expect)

def test_fetch_twse_data(mocker):
    mock_response = {
        "stat": "OK",
        "tables": [0, 0, 0, 0, 0, 0, 0, 0, {
            "fields": ["證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌(+/-)", "漲跌價差", "最後揭示買價", "最後揭示買量", "最後揭示賣價", "最後揭示賣量", "本益比"],
            "data": [["2330", "台積電", "1,234,567", "1,234", "123,456,789", "600", "610", "590", "605", "<p style= color:red>+</p>", "5", "604", "1,000", "605", "2,000", "20"]]
        }]
    }
    mocker.patch('tw_crawler.twse.requests.get', return_value=mocker.Mock(json=lambda: mock_response))
    result = twse.fetch_twse_data("2022-02-18")
    assert result == mock_response

def test_gen_empty_date_df():
    result = twse.gen_empty_date_df()
    expect = pd.DataFrame(columns=twse.en_columns())
    expect.insert(0, "Date", pd.NaT)
    expect = expect.drop(columns=["Dir"])
    pd.testing.assert_frame_equal(result, expect)

def test_parse_twse_data():
    response = {
        "stat": "OK",
        "tables": [0,0,0,0,0,0,0,0,{
            "fields": ["證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌(+/-)", "漲跌價差", "最後揭示買價", "最後揭示買量", "最後揭示賣價", "最後揭示賣量", "本益比"],
            "data": [["2330", "台積電", "1,234,567", "1,234", "123,456,789", "600", "610", "590", "605", "<p style= color:red>+</p>", "5", "604", "1,000", "605", "2,000", "20"]]
        }]
    }
    date = "2022-02-18"
    result = twse.parse_twse_data(response, date)
    expect = pd.DataFrame({
        "Date": pd.to_datetime(["2022-02-18"]),
        "SecurityCode": ["2330"],
        "StockName": ["台積電"],
        "TradeVolume": [1234567],
        "Transaction": [1234],
        "TradeValue": [123456789],
        "OpeningPrice": [600.0],
        "HighestPrice": [610.0],
        "LowestPrice": [590.0],
        "ClosingPrice": [605.0],
        "Change": [5.0],
        "LastBestBidPrice": [604.0],
        "LastBestBidVolume": [1000],
        "LastBestAskPrice": [605.0],
        "LastBestAskVolume": [2000],
        "PriceEarningratio": [20.0]
    })
    pd.testing.assert_frame_equal(result, expect)
    response = {
        "stat": "NG",
        "msgArray": []
    }
    result = twse.parse_twse_data(response, date)
    expect = pd.DataFrame(columns=twse.en_columns())
    expect.insert(0, "Date", pd.NaT)
    expect = expect.drop(columns=["Dir"])
    pd.testing.assert_frame_equal(result, expect)


def test_twse_crawler(mocker):
    mock_response = {
        "stat": "OK",
        "tables": [0,0,0,0,0,0,0,0,{
            "fields": ["證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌(+/-)", "漲跌價差", "最後揭示買價", "最後揭示買量", "最後揭示賣價", "最後揭示賣量", "本益比"],
            "data": [["2330", "台積電", "1,234,567", "1,234", "123,456,789", "600", "610", "590", "605", "<p style= color:red>+</p>", "5", "604", "1,000", "605", "2,000", "20"]]
        }]
    }
    mocker.patch('tw_crawler.twse.requests.get', return_value=mocker.Mock(json=lambda: mock_response))
    result = twse.twse_crawler("2022-02-18")
    expect = pd.DataFrame({
        "Date": pd.to_datetime(["2022-02-18"]),
        "SecurityCode": ["2330"],
        "StockName": ["台積電"],
        "TradeVolume": [1234567],
        "Transaction": [1234],
        "TradeValue": [123456789],
        "OpeningPrice": [600.0],
        "HighestPrice": [610.0],
        "LowestPrice": [590.0],
        "ClosingPrice": [605.0],
        "Change": [5.0],
        "LastBestBidPrice": [604.0],
        "LastBestBidVolume": [1000],
        "LastBestAskPrice": [605.0],
        "LastBestAskVolume": [2000],
        "PriceEarningratio": [20.0]
    })
    pd.testing.assert_frame_equal(result, expect)



