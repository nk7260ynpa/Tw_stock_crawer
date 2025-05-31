import pandas as pd
from tw_crawler.tpex import webzh2en_columns, post_process, fetch_tpex_data, parse_tpex_data, tpex_crawler

def test_webzh2en_columns():
    result = webzh2en_columns()
    expected = {
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
        "最後買量<br>(張數)": "LastBidVolume",
        "最後賣價": "LastBestAskPrice",
        "最後賣量<br>(張數)": "LastBestAskVolume",
        "發行股數 ": "IssuedShares",
        "次日漲停價 ": "NextDayUpLimitPrice",
        "次日跌停價": "NextDayDownLimitPrice",
    }
    assert result == expected

def test_post_process():
    data = {
        "代號": ["1234"],
        "收盤 ": ["1,234.56"],
        "漲跌": ["10"],
        "開盤 ": ["1,200.00"],
        "最高 ": ["1,250.00"],
        "最低": ["1,190.00"],
        "成交股數  ": ["1,000"],
        " 成交金額(元)": ["1,234,560"],
        " 成交筆數 ": ["100"],
        "最後買價": ["1,230.00"],
        "最後買量<br>(張數)": ["10"],
        "最後賣價": ["1,235.00"],
        "最後賣量<br>(張數)": ["20"],
        "發行股數 ": ["10,000"],
        "次日漲停價 ": ["1,300.00"],
        "次日跌停價": ["1,100.00"],
    }
    df = pd.DataFrame(data)
    result = post_process(df)
    expected = pd.DataFrame({
        "Code": ["1234"],
        "Close": [1234.56],
        "Change": [10.0],
        "Open": [1200.00],
        "High": [1250.00],
        "Low": [1190.00],
        "TradeVol(shares)": [1000.0],
        "TradeAmt.(NTD)": [1234560.0],
        "No.ofTransactions": [100],
        "LastBestBidPrice": [1230.00],
        "LastBidVolume": [10.0],
        "LastBestAskPrice": [1235.00],
        "LastBestAskVolume": [20.0],
        "IssuedShares": [10000.0],
        "NextDayUpLimitPrice": [1300.00],
        "NextDayDownLimitPrice": [1100.00],
    })
    pd.testing.assert_frame_equal(result, expected)

def test_fetch_tpex_data(mocker):
    mock_response = {
        "tables": [{
            "fields": ["代號", "名稱", "收盤 ", "漲跌", "開盤 ", "最高 ", "最低", "成交股數  ", " 成交金額(元)", " 成交筆數 ", "最後買價", "最後買量<br>(千股)", "最後賣價", "最後賣量<br>(千股)", "發行股數 ", "次日漲停價 ", "次日跌停價"],
            "data": [["1234", "Test", "1,234.56", "10", "1,200.00", "1,250.00", "1,190.00", "1,000", "1,234,560", "100", "1,230.00", "10", "1,235.00", "20", "10,000", "1,300.00", "1,100.00"]]
        }]
    }
    mocker.patch('tw_crawler.tpex.cloudscraper.create_scraper', return_value=mocker.Mock(post=lambda url, data: mocker.Mock(json=lambda: mock_response)))
    response = fetch_tpex_data("2024-10-29")
    assert response == mock_response

def test_parse_tpex_data():
    response = {
        "tables": [{
            "fields": ["代號", "名稱", "收盤 ", "漲跌", "開盤 ", "最高 ", "最低", "成交股數  ", " 成交金額(元)", " 成交筆數 ", "最後買價", "最後買量<br>(千股)", "最後賣價", "最後賣量<br>(千股)", "發行股數 ", "次日漲停價 ", "次日跌停價"],
            "data": [["1234", "Test", "1,234.56", "10", "1,200.00", "1,250.00", "1,190.00", "1,000", "1,234,560", "100", "1,230.00", "10", "1,235.00", "20", "10,000", "1,300.00", "1,100.00"]]
        }]
    }
    result = parse_tpex_data(response)
    assert not result.empty
    assert result.columns.tolist() == ["代號", "名稱", "收盤 ", "漲跌", "開盤 ", "最高 ", "最低", "成交股數  ", " 成交金額(元)", " 成交筆數 ", "最後買價", "最後買量<br>(千股)", "最後賣價", "最後賣量<br>(千股)", "發行股數 ", "次日漲停價 ", "次日跌停價"]
    expected = pd.DataFrame({
        "代號": ["1234"],
        "名稱": ["Test"],
        "收盤 ": ["1,234.56"],
        "漲跌": ["10"],
        "開盤 ": ["1,200.00"],
        "最高 ": ["1,250.00"],
        "最低": ["1,190.00"],
        "成交股數  ": ["1,000"],
        " 成交金額(元)": ["1,234,560"],
        " 成交筆數 ": ["100"],
        "最後買價": ["1,230.00"],
        "最後買量<br>(千股)": ["10"],
        "最後賣價": ["1,235.00"],
        "最後賣量<br>(千股)": ["20"],
        "發行股數 ": ["10,000"],
        "次日漲停價 ": ["1,300.00"],
        "次日跌停價": ["1,100.00"],
    })
    pd.testing.assert_frame_equal(result, expected)

def test_tpex_crawler(mocker):
    mock_response = {
        "tables": [{
            "fields": ["代號", "名稱", "收盤 ", "漲跌", "開盤 ", "最高 ", "最低", "成交股數  ", " 成交金額(元)", " 成交筆數 ", "最後買價", "最後買量<br>(張數)", "最後賣價", "最後賣量<br>(張數)", "發行股數 ", "次日漲停價 ", "次日跌停價"],
            "data": [["1234", "Test", "1,234.56", "10", "1,200.00", "1,250.00", "1,190.00", "1,000", "1,234,560", "100", "1,230.00", "10", "1,235.00", "20", "10,000", "1,300.00", "1,100.00"]]
        }]
    }
    mocker.patch('tw_crawler.tpex.fetch_tpex_data', return_value=mock_response)
    result = tpex_crawler("2024-10-29")
    assert not result.empty
    expected = pd.DataFrame({
        "Code": ["1234"],
        "Name": ["Test"],
        "Close": [1234.56],
        "Change": [10.0],
        "Open": [1200.00],
        "High": [1250.00],
        "Low": [1190.00],
        "TradeVol(shares)": [1000.0],
        "TradeAmt.(NTD)": [1234560.0],
        "No.ofTransactions": [100],
        "LastBestBidPrice": [1230.00],
        "LastBidVolume": [10.0],
        "LastBestAskPrice": [1235.00],
        "LastBestAskVolume": [20.0],
        "IssuedShares": [10000.0],
        "NextDayUpLimitPrice": [1300.00],
        "NextDayDownLimitPrice": [1100.00],
    })
    pd.testing.assert_frame_equal(result, expected)