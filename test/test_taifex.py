"""TAIFEX 期貨爬蟲測試模組。"""

import pytest
import pandas as pd
import tw_crawler.taifex as taifex

def test_webzh2en_columns():
    columns = taifex.webzh2en_columns()
    assert isinstance(columns, dict)
    expect = {
        "交易日期": "Date",
        "契約": "Contract",
        "到期月份(週別)": "ContractMonth",
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
    assert columns == expect

def test_post_process():
    data = {
        "交易日期": ["2024/10/29"],
        "契約": ["TX"],
        "到期月份(週別)": ["202410"],
        "開盤價": ["10000"],
        "最高價": ["10100"],
        "最低價": ["9900"],
        "收盤價": ["10050"],
        "漲跌價": ["50"],
        "漲跌%": ["0.5%"],
        "成交量": ["1000"],
        "結算價": ["10050"],
        "未沖銷契約數": ["5000"],
        "最後最佳買價": ["10040"],
        "最後最佳賣價": ["10060"],
        "歷史最高價": ["11000"],
        "歷史最低價": ["9000"],
        "是否因訊息面暫停交易": ["否"],
        "交易時段": ["一般"],
        "價差對單式委託成交量": ["100"]
    }
    df = pd.DataFrame(data)
    processed_df = taifex.post_process(df)
    assert "Date" in processed_df.columns
    assert processed_df["Date"].dtype == "datetime64[ns]"
    expect = pd.DataFrame({
        "Date": [pd.Timestamp("2024-10-29")],
        "Contract": ["TX"],
        "ContractMonth": ["202410"],
        "Open": [10000.0],
        "High": [10100.0],
        "Low": [9900.0],
        "Last": [10050.0],
        "Change": [50.0],
        "ChangePercent": [0.005],
        "Volume": [1000],
        "SettlementPrice": [10050.0],
        "OpenInterest": [5000.0],
        "BestBid": [10040.0],
        "BestAsk": [10060.0],
        "HistoricalHigh": [11000.0],
        "HistoricalLow": [9000.0],
        "TradingHalt": [0.0],
        "TradingSession": ["一般"],
        "SpreadOrderVolume": [100.0]
    })
    pd.testing.assert_frame_equal(processed_df, expect)

def test_fetch_taifex_data(mocker):
    mock_response = mocker.Mock()
    mock_response.text = "交易日期,契約,到期月份(週別),開盤價,最高價,最低價,收盤價,漲跌價,漲跌%,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,價差對單式委託成交量\n2024/10/29,TX,202410,10000,10100,9900,10050,50,0.5%,1000,10050,5000,10040,10060,11000,9000,否,一般,100"
    mocker.patch("cloudscraper.create_scraper", return_value=mocker.Mock(post=mocker.Mock(return_value=mock_response)))
    response = taifex.fetch_taifex_data("2024-10-29")
    assert "交易日期" in response
    assert response == mock_response.text


def test_parse_taifex_data():
    response = "交易日期,契約,到期月份(週別),開盤價,最高價,最低價,收盤價,漲跌價,漲跌%,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,價差對單式委託成交量\n2024/10/29,TX,202410,10000,10100,9900,10050,50,0.5%,1000,10050,5000,10040,10060,11000,9000,否,一般,100"
    df = taifex.parse_taifex_data(response)
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 1
    expect = pd.DataFrame({
        "交易日期": ["2024/10/29"],
        "契約": ["TX"],
        "到期月份(週別)": [202410],
        "開盤價": [10000],
        "最高價": [10100],
        "最低價": [9900],
        "收盤價": [10050],
        "漲跌價": [50],
        "漲跌%": ["0.5%"],
        "成交量": [1000],
        "結算價": [10050],
        "未沖銷契約數": [5000],
        "最後最佳買價": [10040],
        "最後最佳賣價": [10060],
        "歷史最高價": [11000],
        "歷史最低價": [9000],
        "是否因訊息面暫停交易": ["否"],
        "交易時段": ["一般"],
        "價差對單式委託成交量": [100]
    })
    pd.testing.assert_frame_equal(df, expect)


def test_taifex_crawler(mocker):
    mock_response = "交易日期,契約,到期月份(週別),開盤價,最高價,最低價,收盤價,漲跌價,漲跌%,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,價差對單式委託成交量\n2024/10/29,TX,202410,10000,10100,9900,10050,50,0.5%,1000,10050,5000,10040,10060,11000,9000,否,一般,100"
    mocker.patch("tw_crawler.taifex.fetch_taifex_data", return_value=mock_response)
    df = taifex.taifex_crawler("2024-10-29")
    assert isinstance(df, pd.DataFrame)
    assert "Date" in df.columns
    assert df["Date"].iloc[0] == pd.Timestamp("2024-10-29")
    expect = pd.DataFrame({
        "Date": [pd.Timestamp("2024-10-29")],
        "Contract": ["TX"],
        "ContractMonth": ["202410"],
        "Open": [10000.0],
        "High": [10100.0],
        "Low": [9900.0],
        "Last": [10050.0],
        "Change": [50.0],
        "ChangePercent": [0.005],
        "Volume": [1000],
        "SettlementPrice": [10050.0],
        "OpenInterest": [5000.0],
        "BestBid": [10040.0],
        "BestAsk": [10060.0],
        "HistoricalHigh": [11000.0],
        "HistoricalLow": [9000.0],
        "TradingHalt": [0.0],
        "TradingSession": ["一般"],
        "SpreadOrderVolume": [100.0]
    })
    pd.testing.assert_frame_equal(df, expect)
