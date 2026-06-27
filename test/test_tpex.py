"""TPEX 上櫃股票爬蟲測試模組。"""

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from tw_crawler._http import NonJsonResponseError
from tw_crawler.tpex import (
    fetch_tpex_data,
    parse_tpex_data,
    post_process,
    tpex_crawler,
    webzh2en_columns,
)


def test_webzh2en_columns() -> None:
    result = webzh2en_columns()
    expected = {
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
    assert result == expected


def test_post_process() -> None:
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
    result = post_process(df, "2024-10-29")
    expected = pd.DataFrame({
        "Date": ["2024-10-29"],
        "Code": ["1234"],
        "Close": [1234.56],
        "Change": [10.0],
        "Open": [1200.00],
        "High": [1250.00],
        "Low": [1190.00],
        "TradeVolume": [1000],
        "TradeAmount": [1234560],
        "NumberOfTransactions": [100],
        "LastBestBidPrice": [1230.00],
        "LastBidVolume": [10],
        "LastBestAskPrice": [1235.00],
        "LastBestAskVolume": [20],
        "IssuedShares": [10000],
        "NextDayUpLimitPrice": [1300.00],
        "NextDayDownLimitPrice": [1100.00],
    })
    expected["Date"] = pd.to_datetime(expected["Date"])
    pd.testing.assert_frame_equal(result, expected)


def test_post_process_change_with_comma_sign_and_xrxd() -> None:
    """漲跌欄位含千分位逗號、正負號、或「除權息」字串時皆需正確解析。"""
    base = {
        "代號": ["1234", "5678", "9012", "3456"],
        "收盤 ": ["1,234.56", "1,000.00", "999.00", "500.00"],
        "漲跌": ["+1,070.00", "-1,045.00", "除權息", "10.5"],
        "開盤 ": ["1,200.00", "1,000.00", "999.00", "500.00"],
        "最高 ": ["1,250.00", "1,000.00", "999.00", "500.00"],
        "最低": ["1,190.00", "1,000.00", "999.00", "500.00"],
        "成交股數  ": ["1,000", "1,000", "1,000", "1,000"],
        " 成交金額(元)": ["1,234,560", "1", "1", "1"],
        " 成交筆數 ": ["100", "1", "1", "1"],
        "最後買價": ["1,230.00", "1.0", "1.0", "1.0"],
        "最後買量<br>(張數)": ["10", "1", "1", "1"],
        "最後賣價": ["1,235.00", "1.0", "1.0", "1.0"],
        "最後賣量<br>(張數)": ["20", "1", "1", "1"],
        "發行股數 ": ["10,000", "1", "1", "1"],
        "次日漲停價 ": ["1,300.00", "1.0", "1.0", "1.0"],
        "次日跌停價": ["1,100.00", "1.0", "1.0", "1.0"],
    }
    df = pd.DataFrame(base)
    result = post_process(df, "2026-05-06")
    assert result.loc[0, "Change"] == 1070.0
    assert result.loc[1, "Change"] == -1045.0
    assert pd.isna(result.loc[2, "Change"])
    assert result.loc[3, "Change"] == 10.5


TPEX_FIELDS_THOUSAND = [
    "代號", "名稱", "收盤 ", "漲跌", "開盤 ", "最高 ", "最低",
    "成交股數  ", " 成交金額(元)", " 成交筆數 ", "最後買價",
    "最後買量<br>(千股)", "最後賣價", "最後賣量<br>(千股)",
    "發行股數 ", "次日漲停價 ", "次日跌停價",
]

TPEX_FIELDS_SHEET = [
    "代號", "名稱", "收盤 ", "漲跌", "開盤 ", "最高 ", "最低",
    "成交股數  ", " 成交金額(元)", " 成交筆數 ", "最後買價",
    "最後買量<br>(張數)", "最後賣價", "最後賣量<br>(張數)",
    "發行股數 ", "次日漲停價 ", "次日跌停價",
]

TPEX_SAMPLE_DATA = [[
    "1234", "Test", "1,234.56", "10", "1,200.00", "1,250.00",
    "1,190.00", "1,000", "1,234,560", "100", "1,230.00", "10",
    "1,235.00", "20", "10,000", "1,300.00", "1,100.00",
]]


def test_fetch_tpex_data(mocker: MockerFixture) -> None:
    """正常情況下 fetch_tpex_data 透過 safe_post_json 取得 JSON。"""
    mock_response = {
        "tables": [{
            "fields": TPEX_FIELDS_THOUSAND,
            "data": TPEX_SAMPLE_DATA,
        }]
    }
    mock_post_result = mocker.Mock(
        status_code=200,
        json=lambda: mock_response,
    )
    mock_scraper = mocker.Mock()
    mock_scraper.post.return_value = mock_post_result
    mocker.patch(
        'tw_crawler.tpex.cloudscraper.create_scraper',
        return_value=mock_scraper,
    )
    response = fetch_tpex_data("2024-10-29")
    assert response == mock_response


def test_fetch_tpex_data_non_json_raises_clear_error(
    mocker: MockerFixture,
) -> None:
    """TPEX 回傳非 JSON（HTML 錯誤頁／被擋）時應拋出帶內文節錄的清楚例外。

    對應 06-27 線上故障：``scraper.post(...).json()`` 在收到非 JSON 時會拋出
    不透明的 ``Expecting value: line 1 column 1 (char 0)``；改用 safe_post_json
    後應改拋 NonJsonResponseError 並在訊息中節錄實際回應內文。
    """
    html_body = (
        "<html><body>403 Forbidden - access denied by WAF</body></html>"
    )

    def _raise_json() -> None:
        raise ValueError("Expecting value: line 1 column 1 (char 0)")

    mock_post_result = mocker.Mock(
        status_code=200,
        text=html_body,
        json=_raise_json,
    )
    mock_scraper = mocker.Mock()
    mock_scraper.post.return_value = mock_post_result
    mocker.patch(
        'tw_crawler.tpex.cloudscraper.create_scraper',
        return_value=mock_scraper,
    )
    # 避免重試時真的 sleep，加速測試。
    mocker.patch('tw_crawler._http.time.sleep')

    with pytest.raises(NonJsonResponseError) as exc_info:
        fetch_tpex_data("2024-10-29")

    message = str(exc_info.value)
    assert "TPEX 上櫃股票資料" in message
    assert "非 JSON" in message
    assert "403 Forbidden" in message  # 內文節錄可見實際回應


def test_fetch_tpex_data_non_2xx_raises_clear_error(
    mocker: MockerFixture,
) -> None:
    """TPEX 回應非 2xx 狀態碼時應拋出帶狀態碼與內文節錄的清楚例外。"""
    mock_post_result = mocker.Mock(
        status_code=503,
        text="Service Temporarily Unavailable",
        json=lambda: {},
    )
    mock_scraper = mocker.Mock()
    mock_scraper.post.return_value = mock_post_result
    mocker.patch(
        'tw_crawler.tpex.cloudscraper.create_scraper',
        return_value=mock_scraper,
    )
    mocker.patch('tw_crawler._http.time.sleep')

    with pytest.raises(NonJsonResponseError) as exc_info:
        fetch_tpex_data("2024-10-29")

    message = str(exc_info.value)
    assert "503" in message
    assert "Service Temporarily Unavailable" in message


def test_parse_tpex_data() -> None:
    response = {
        "tables": [{
            "fields": TPEX_FIELDS_THOUSAND,
            "data": TPEX_SAMPLE_DATA,
        }]
    }
    result = parse_tpex_data(response)
    assert not result.empty
    assert result.columns.tolist() == TPEX_FIELDS_THOUSAND
    expected = pd.DataFrame(
        columns=TPEX_FIELDS_THOUSAND,
        data=TPEX_SAMPLE_DATA,
    )
    pd.testing.assert_frame_equal(result, expected)


def test_tpex_crawler(mocker: MockerFixture) -> None:
    mock_response = {
        "tables": [{
            "fields": TPEX_FIELDS_SHEET,
            "data": TPEX_SAMPLE_DATA,
        }]
    }
    mocker.patch(
        'tw_crawler.tpex.fetch_tpex_data',
        return_value=mock_response,
    )
    result = tpex_crawler("2024-10-29")
    assert not result.empty
    expected = pd.DataFrame({
        "Date": ["2024-10-29"],
        "Code": ["1234"],
        "Name": ["Test"],
        "Close": [1234.56],
        "Change": [10.0],
        "Open": [1200.00],
        "High": [1250.00],
        "Low": [1190.00],
        "TradeVolume": [1000],
        "TradeAmount": [1234560],
        "NumberOfTransactions": [100],
        "LastBestBidPrice": [1230.00],
        "LastBidVolume": [10],
        "LastBestAskPrice": [1235.00],
        "LastBestAskVolume": [20],
        "IssuedShares": [10000],
        "NextDayUpLimitPrice": [1300.00],
        "NextDayDownLimitPrice": [1100.00],
    })
    expected["Date"] = pd.to_datetime(expected["Date"])
    pd.testing.assert_frame_equal(result, expected)
