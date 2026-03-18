"""國際原油價格爬蟲測試模組。"""

import pandas as pd
import pytest
from pytest_mock import MockerFixture

import tw_crawler.oil_price as oil_price


def _make_history_df() -> pd.DataFrame:
    """建立模擬的 yfinance 歷史資料 DataFrame。"""
    index = pd.DatetimeIndex(
        ["2026-03-16", "2026-03-17", "2026-03-18"],
        tz="America/New_York",
    )
    return pd.DataFrame(
        {
            "Open": [67.50, 68.00, 68.50],
            "High": [68.20, 69.00, 69.20],
            "Low": [67.10, 67.80, 68.10],
            "Close": [68.00, 68.50, 68.85],
            "Volume": [230000, 240000, 250000],
            "Dividends": [0.0, 0.0, 0.0],
            "Stock Splits": [0.0, 0.0, 0.0],
        },
        index=index,
    )


def test_oil_tickers() -> None:
    """測試原油 ticker 對照表包含 WTI 和 Brent。"""
    assert "WTI" in oil_price.OIL_TICKERS
    assert "Brent" in oil_price.OIL_TICKERS
    assert oil_price.OIL_TICKERS["WTI"] == "CL=F"
    assert oil_price.OIL_TICKERS["Brent"] == "BZ=F"


def test_fetch_oil_data(mocker: MockerFixture) -> None:
    """測試 fetch_oil_data 正確呼叫 yfinance API。"""
    mock_df = _make_history_df()
    mock_ticker = mocker.Mock()
    mock_ticker.history.return_value = mock_df
    mocker.patch(
        "tw_crawler.oil_price.yf.Ticker",
        return_value=mock_ticker,
    )

    result = oil_price.fetch_oil_data("CL=F", "2026-03-18")

    oil_price.yf.Ticker.assert_called_once_with("CL=F")
    mock_ticker.history.assert_called_once_with(
        start="2026-03-11", end="2026-03-19"
    )
    pd.testing.assert_frame_equal(result, mock_df)


def test_parse_oil_data_normal() -> None:
    """測試正常解析原油價格資料。"""
    df = _make_history_df()
    result = oil_price.parse_oil_data(df, "WTI", "2026-03-18")

    assert result is not None
    assert result["product"] == "WTI"
    assert result["date"] == "2026-03-18"
    assert result["open"] == 68.50
    assert result["high"] == 69.20
    assert result["low"] == 68.10
    assert result["close"] == 68.85
    assert result["volume"] == 250000


def test_parse_oil_data_holiday() -> None:
    """測試查詢假日時回傳最近交易日的資料。"""
    df = _make_history_df()
    # 查詢 2026-03-19（假設無交易），應回傳 03-18 的資料
    result = oil_price.parse_oil_data(df, "Brent", "2026-03-19")

    assert result is not None
    assert result["date"] == "2026-03-18"
    assert result["close"] == 68.85


def test_parse_oil_data_specific_date() -> None:
    """測試查詢特定日期，只回傳該日或之前的資料。"""
    df = _make_history_df()
    # 查詢 2026-03-17，不應取到 03-18 的資料
    result = oil_price.parse_oil_data(df, "WTI", "2026-03-17")

    assert result is not None
    assert result["date"] == "2026-03-17"
    assert result["close"] == 68.50


def test_parse_oil_data_empty() -> None:
    """測試空的 DataFrame 回傳 None。"""
    df = pd.DataFrame()
    result = oil_price.parse_oil_data(df, "WTI", "2026-03-18")

    assert result is None


def test_parse_oil_data_no_match() -> None:
    """測試查詢日期早於所有資料時回傳 None。"""
    df = _make_history_df()
    result = oil_price.parse_oil_data(df, "WTI", "2026-03-10")

    assert result is None


def test_oil_price_crawler_success(mocker: MockerFixture) -> None:
    """測試 oil_price_crawler 成功回傳 WTI 和 Brent 資料。"""
    mock_df = _make_history_df()
    mocker.patch(
        "tw_crawler.oil_price.fetch_oil_data",
        return_value=mock_df,
    )

    result = oil_price.oil_price_crawler("2026-03-18")

    assert len(result) == 2
    products = [r["product"] for r in result]
    assert "WTI" in products
    assert "Brent" in products

    wti = next(r for r in result if r["product"] == "WTI")
    assert wti["close"] == 68.85
    assert wti["volume"] == 250000


def test_oil_price_crawler_partial_failure(
    mocker: MockerFixture,
) -> None:
    """測試一個商品失敗時，仍回傳另一個成功的商品。"""
    mock_df = _make_history_df()
    call_count = 0

    def _side_effect(ticker: str, date: str) -> pd.DataFrame:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Network error")
        return mock_df

    mocker.patch(
        "tw_crawler.oil_price.fetch_oil_data",
        side_effect=_side_effect,
    )

    result = oil_price.oil_price_crawler("2026-03-18")

    assert len(result) == 1


def test_oil_price_crawler_all_fail(mocker: MockerFixture) -> None:
    """測試所有商品都失敗時拋出 ValueError。"""
    mocker.patch(
        "tw_crawler.oil_price.fetch_oil_data",
        side_effect=ConnectionError("Network error"),
    )

    with pytest.raises(ValueError, match="無法取得任何原油價格資料"):
        oil_price.oil_price_crawler("2026-03-18")


def test_oil_price_crawler_all_empty(mocker: MockerFixture) -> None:
    """測試所有商品回傳空資料時拋出 ValueError。"""
    mocker.patch(
        "tw_crawler.oil_price.fetch_oil_data",
        return_value=pd.DataFrame(),
    )

    with pytest.raises(ValueError, match="無法取得任何原油價格資料"):
        oil_price.oil_price_crawler("2026-03-18")
