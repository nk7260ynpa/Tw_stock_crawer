"""比特幣價格爬蟲測試模組。"""

import pandas as pd
import pytest
from pytest_mock import MockerFixture

import tw_crawler.bitcoin_price as bitcoin_price


def _make_history_df() -> pd.DataFrame:
    """建立模擬的 yfinance 歷史資料 DataFrame。"""
    index = pd.DatetimeIndex(
        ["2026-03-16", "2026-03-17", "2026-03-18"],
        tz="UTC",
    )
    return pd.DataFrame(
        {
            "Open": [82500.00, 83000.00, 83500.00],
            "High": [83200.00, 84000.00, 84500.00],
            "Low": [82100.00, 82800.00, 83200.00],
            "Close": [83000.00, 83500.00, 84200.00],
            "Volume": [35000000000, 36000000000, 37000000000],
            "Dividends": [0.0, 0.0, 0.0],
            "Stock Splits": [0.0, 0.0, 0.0],
        },
        index=index,
    )


def test_bitcoin_tickers() -> None:
    """測試比特幣 ticker 對照表包含 Bitcoin。"""
    assert "Bitcoin" in bitcoin_price.BITCOIN_TICKERS
    assert bitcoin_price.BITCOIN_TICKERS["Bitcoin"] == "BTC-USD"


def test_fetch_bitcoin_data(mocker: MockerFixture) -> None:
    """測試 fetch_bitcoin_data 正確呼叫 yfinance API。"""
    mock_df = _make_history_df()
    mock_ticker = mocker.Mock()
    mock_ticker.history.return_value = mock_df
    mocker.patch(
        "tw_crawler.bitcoin_price.yf.Ticker",
        return_value=mock_ticker,
    )

    result = bitcoin_price.fetch_bitcoin_data("BTC-USD", "2026-03-18")

    bitcoin_price.yf.Ticker.assert_called_once_with("BTC-USD")
    mock_ticker.history.assert_called_once_with(
        start="2026-03-11", end="2026-03-19"
    )
    pd.testing.assert_frame_equal(result, mock_df)


def test_parse_bitcoin_data_normal() -> None:
    """測試正常解析比特幣價格資料。"""
    df = _make_history_df()
    result = bitcoin_price.parse_bitcoin_data(
        df, "Bitcoin", "2026-03-18"
    )

    assert result is not None
    assert result["product"] == "Bitcoin"
    assert result["date"] == "2026-03-18"
    assert result["open"] == 83500.00
    assert result["high"] == 84500.00
    assert result["low"] == 83200.00
    assert result["close"] == 84200.00
    assert result["volume"] == 37000000000


def test_parse_bitcoin_data_holiday() -> None:
    """測試查詢假日時回傳最近交易日的資料。"""
    df = _make_history_df()
    # 查詢 2026-03-19（假設無交易），應回傳 03-18 的資料
    result = bitcoin_price.parse_bitcoin_data(
        df, "Bitcoin", "2026-03-19"
    )

    assert result is not None
    assert result["date"] == "2026-03-18"
    assert result["close"] == 84200.00


def test_parse_bitcoin_data_specific_date() -> None:
    """測試查詢特定日期，只回傳該日或之前的資料。"""
    df = _make_history_df()
    # 查詢 2026-03-17，不應取到 03-18 的資料
    result = bitcoin_price.parse_bitcoin_data(
        df, "Bitcoin", "2026-03-17"
    )

    assert result is not None
    assert result["date"] == "2026-03-17"
    assert result["close"] == 83500.00


def test_parse_bitcoin_data_empty() -> None:
    """測試空的 DataFrame 回傳 None。"""
    df = pd.DataFrame()
    result = bitcoin_price.parse_bitcoin_data(
        df, "Bitcoin", "2026-03-18"
    )

    assert result is None


def test_parse_bitcoin_data_no_match() -> None:
    """測試查詢日期早於所有資料時回傳 None。"""
    df = _make_history_df()
    result = bitcoin_price.parse_bitcoin_data(
        df, "Bitcoin", "2026-03-10"
    )

    assert result is None


def test_bitcoin_price_crawler_success(
    mocker: MockerFixture,
) -> None:
    """測試 bitcoin_price_crawler 成功回傳 Bitcoin 資料。"""
    mock_df = _make_history_df()
    mocker.patch(
        "tw_crawler.bitcoin_price.fetch_bitcoin_data",
        return_value=mock_df,
    )

    result = bitcoin_price.bitcoin_price_crawler("2026-03-18")

    assert len(result) == 1
    assert result[0]["product"] == "Bitcoin"
    assert result[0]["close"] == 84200.00
    assert result[0]["volume"] == 37000000000


def test_bitcoin_price_crawler_all_fail(
    mocker: MockerFixture,
) -> None:
    """測試所有商品都失敗時拋出 ValueError。"""
    mocker.patch(
        "tw_crawler.bitcoin_price.fetch_bitcoin_data",
        side_effect=ConnectionError("Network error"),
    )

    with pytest.raises(
        ValueError, match="無法取得任何比特幣價格資料"
    ):
        bitcoin_price.bitcoin_price_crawler("2026-03-18")


def test_bitcoin_price_crawler_all_empty(
    mocker: MockerFixture,
) -> None:
    """測試所有商品回傳空資料時拋出 ValueError。"""
    mocker.patch(
        "tw_crawler.bitcoin_price.fetch_bitcoin_data",
        return_value=pd.DataFrame(),
    )

    with pytest.raises(
        ValueError, match="無法取得任何比特幣價格資料"
    ):
        bitcoin_price.bitcoin_price_crawler("2026-03-18")
