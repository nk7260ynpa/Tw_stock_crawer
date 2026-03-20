"""國際股市指數爬蟲測試模組。"""

import numpy as np
import pandas as pd
import pytest
from pytest_mock import MockerFixture

import tw_crawler.indices_price as indices_price


def _make_history_df() -> pd.DataFrame:
    """建立模擬的 yfinance 歷史資料 DataFrame。"""
    index = pd.DatetimeIndex(
        ["2026-03-16", "2026-03-17", "2026-03-18"],
        tz="America/New_York",
    )
    return pd.DataFrame(
        {
            "Open": [42500.00, 42600.00, 42700.00],
            "High": [42800.00, 42900.00, 43000.00],
            "Low": [42400.00, 42500.00, 42600.00],
            "Close": [42700.00, 42800.00, 42900.00],
            "Volume": [350000000, 360000000, 370000000],
            "Dividends": [0.0, 0.0, 0.0],
            "Stock Splits": [0.0, 0.0, 0.0],
        },
        index=index,
    )


def _make_history_df_nan_volume() -> pd.DataFrame:
    """建立 Volume 為 NaN 的模擬 DataFrame。"""
    index = pd.DatetimeIndex(
        ["2026-03-18"],
        tz="America/New_York",
    )
    return pd.DataFrame(
        {
            "Open": [42700.00],
            "High": [43000.00],
            "Low": [42600.00],
            "Close": [42900.00],
            "Volume": [np.nan],
            "Dividends": [0.0],
            "Stock Splits": [0.0],
        },
        index=index,
    )


def test_indices_tickers() -> None:
    """測試股市指數 ticker 對照表包含 DowJones 和 Nasdaq。"""
    assert "DowJones" in indices_price.INDICES_TICKERS
    assert "Nasdaq" in indices_price.INDICES_TICKERS
    assert indices_price.INDICES_TICKERS["DowJones"] == "^DJI"
    assert indices_price.INDICES_TICKERS["Nasdaq"] == "^IXIC"


def test_fetch_indices_data(mocker: MockerFixture) -> None:
    """測試 fetch_indices_data 正確呼叫 yfinance API。"""
    mock_df = _make_history_df()
    mock_ticker = mocker.Mock()
    mock_ticker.history.return_value = mock_df
    mocker.patch(
        "tw_crawler.indices_price.yf.Ticker",
        return_value=mock_ticker,
    )

    result = indices_price.fetch_indices_data("^DJI", "2026-03-18")

    indices_price.yf.Ticker.assert_called_once_with("^DJI")
    mock_ticker.history.assert_called_once_with(
        start="2026-03-11", end="2026-03-19"
    )
    pd.testing.assert_frame_equal(result, mock_df)


def test_parse_indices_data_normal() -> None:
    """測試正常解析股市指數價格資料。"""
    df = _make_history_df()
    result = indices_price.parse_indices_data(df, "DowJones", "2026-03-18")

    assert result is not None
    assert result["product"] == "DowJones"
    assert result["date"] == "2026-03-18"
    assert result["open"] == 42700.00
    assert result["high"] == 43000.00
    assert result["low"] == 42600.00
    assert result["close"] == 42900.00
    assert result["volume"] == 370000000


def test_parse_indices_data_holiday() -> None:
    """測試查詢假日時回傳最近交易日的資料。"""
    df = _make_history_df()
    # 查詢 2026-03-19（假設無交易），應回傳 03-18 的資料
    result = indices_price.parse_indices_data(df, "Nasdaq", "2026-03-19")

    assert result is not None
    assert result["date"] == "2026-03-18"
    assert result["close"] == 42900.00


def test_parse_indices_data_specific_date() -> None:
    """測試查詢特定日期，只回傳該日或之前的資料。"""
    df = _make_history_df()
    # 查詢 2026-03-17，不應取到 03-18 的資料
    result = indices_price.parse_indices_data(df, "DowJones", "2026-03-17")

    assert result is not None
    assert result["date"] == "2026-03-17"
    assert result["close"] == 42800.00


def test_parse_indices_data_empty() -> None:
    """測試空的 DataFrame 回傳 None。"""
    df = pd.DataFrame()
    result = indices_price.parse_indices_data(df, "DowJones", "2026-03-18")

    assert result is None


def test_parse_indices_data_no_match() -> None:
    """測試查詢日期早於所有資料時回傳 None。"""
    df = _make_history_df()
    result = indices_price.parse_indices_data(df, "DowJones", "2026-03-10")

    assert result is None


def test_parse_indices_data_nan_volume() -> None:
    """測試 Volume 為 NaN 時回傳 0。"""
    df = _make_history_df_nan_volume()
    result = indices_price.parse_indices_data(df, "Nasdaq", "2026-03-18")

    assert result is not None
    assert result["volume"] == 0
    assert result["close"] == 42900.00


def test_indices_price_crawler_success(mocker: MockerFixture) -> None:
    """測試 indices_price_crawler 成功回傳 DowJones 和 Nasdaq 資料。"""
    mock_df = _make_history_df()
    mocker.patch(
        "tw_crawler.indices_price.fetch_indices_data",
        return_value=mock_df,
    )

    result = indices_price.indices_price_crawler("2026-03-18")

    assert len(result) == 2
    products = [r["product"] for r in result]
    assert "DowJones" in products
    assert "Nasdaq" in products

    dow = next(r for r in result if r["product"] == "DowJones")
    assert dow["close"] == 42900.00
    assert dow["volume"] == 370000000


def test_indices_price_crawler_partial_failure(
    mocker: MockerFixture,
) -> None:
    """測試一個指數失敗時，仍回傳另一個成功的指數。"""
    mock_df = _make_history_df()
    call_count = 0

    def _side_effect(ticker: str, date: str) -> pd.DataFrame:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Network error")
        return mock_df

    mocker.patch(
        "tw_crawler.indices_price.fetch_indices_data",
        side_effect=_side_effect,
    )

    result = indices_price.indices_price_crawler("2026-03-18")

    assert len(result) == 1


def test_indices_price_crawler_all_fail(mocker: MockerFixture) -> None:
    """測試所有指數都失敗時拋出 ValueError。"""
    mocker.patch(
        "tw_crawler.indices_price.fetch_indices_data",
        side_effect=ConnectionError("Network error"),
    )

    with pytest.raises(ValueError, match="無法取得任何股市指數資料"):
        indices_price.indices_price_crawler("2026-03-18")


def test_indices_price_crawler_all_empty(mocker: MockerFixture) -> None:
    """測試所有指數回傳空資料時拋出 ValueError。"""
    mocker.patch(
        "tw_crawler.indices_price.fetch_indices_data",
        return_value=pd.DataFrame(),
    )

    with pytest.raises(ValueError, match="無法取得任何股市指數資料"):
        indices_price.indices_price_crawler("2026-03-18")
