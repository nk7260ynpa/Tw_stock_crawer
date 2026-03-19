"""國際黃金價格爬蟲測試模組。"""

import pandas as pd
import pytest
from pytest_mock import MockerFixture

import tw_crawler.gold_price as gold_price


def _make_history_df() -> pd.DataFrame:
    """建立模擬的 yfinance 歷史資料 DataFrame。"""
    index = pd.DatetimeIndex(
        ["2026-03-16", "2026-03-17", "2026-03-18"],
        tz="America/New_York",
    )
    return pd.DataFrame(
        {
            "Open": [2650.50, 2665.00, 2680.00],
            "High": [2670.00, 2685.00, 2695.00],
            "Low": [2645.00, 2660.00, 2675.00],
            "Close": [2665.00, 2680.00, 2690.50],
            "Volume": [150000, 160000, 170000],
            "Dividends": [0.0, 0.0, 0.0],
            "Stock Splits": [0.0, 0.0, 0.0],
        },
        index=index,
    )


def test_gold_tickers() -> None:
    """測試黃金 ticker 對照表包含 Gold。"""
    assert "Gold" in gold_price.GOLD_TICKERS
    assert gold_price.GOLD_TICKERS["Gold"] == "GC=F"


def test_fetch_gold_data(mocker: MockerFixture) -> None:
    """測試 fetch_gold_data 正確呼叫 yfinance API。"""
    mock_df = _make_history_df()
    mock_ticker = mocker.Mock()
    mock_ticker.history.return_value = mock_df
    mocker.patch(
        "tw_crawler.gold_price.yf.Ticker",
        return_value=mock_ticker,
    )

    result = gold_price.fetch_gold_data("GC=F", "2026-03-18")

    gold_price.yf.Ticker.assert_called_once_with("GC=F")
    mock_ticker.history.assert_called_once_with(
        start="2026-03-11", end="2026-03-19"
    )
    pd.testing.assert_frame_equal(result, mock_df)


def test_parse_gold_data_normal() -> None:
    """測試正常解析黃金價格資料。"""
    df = _make_history_df()
    result = gold_price.parse_gold_data(df, "Gold", "2026-03-18")

    assert result is not None
    assert result["product"] == "Gold"
    assert result["date"] == "2026-03-18"
    assert result["open"] == 2680.00
    assert result["high"] == 2695.00
    assert result["low"] == 2675.00
    assert result["close"] == 2690.50
    assert result["volume"] == 170000


def test_parse_gold_data_holiday() -> None:
    """測試查詢假日時回傳最近交易日的資料。"""
    df = _make_history_df()
    # 查詢 2026-03-19（假設無交易），應回傳 03-18 的資料
    result = gold_price.parse_gold_data(df, "Gold", "2026-03-19")

    assert result is not None
    assert result["date"] == "2026-03-18"
    assert result["close"] == 2690.50


def test_parse_gold_data_specific_date() -> None:
    """測試查詢特定日期，只回傳該日或之前的資料。"""
    df = _make_history_df()
    # 查詢 2026-03-17，不應取到 03-18 的資料
    result = gold_price.parse_gold_data(df, "Gold", "2026-03-17")

    assert result is not None
    assert result["date"] == "2026-03-17"
    assert result["close"] == 2680.00


def test_parse_gold_data_empty() -> None:
    """測試空的 DataFrame 回傳 None。"""
    df = pd.DataFrame()
    result = gold_price.parse_gold_data(df, "Gold", "2026-03-18")

    assert result is None


def test_parse_gold_data_no_match() -> None:
    """測試查詢日期早於所有資料時回傳 None。"""
    df = _make_history_df()
    result = gold_price.parse_gold_data(df, "Gold", "2026-03-10")

    assert result is None


def test_gold_price_crawler_success(mocker: MockerFixture) -> None:
    """測試 gold_price_crawler 成功回傳 Gold 資料。"""
    mock_df = _make_history_df()
    mocker.patch(
        "tw_crawler.gold_price.fetch_gold_data",
        return_value=mock_df,
    )

    result = gold_price.gold_price_crawler("2026-03-18")

    assert len(result) == 1
    assert result[0]["product"] == "Gold"
    assert result[0]["close"] == 2690.50
    assert result[0]["volume"] == 170000


def test_gold_price_crawler_all_fail(mocker: MockerFixture) -> None:
    """測試所有商品都失敗時拋出 ValueError。"""
    mocker.patch(
        "tw_crawler.gold_price.fetch_gold_data",
        side_effect=ConnectionError("Network error"),
    )

    with pytest.raises(ValueError, match="無法取得任何黃金價格資料"):
        gold_price.gold_price_crawler("2026-03-18")


def test_gold_price_crawler_all_empty(mocker: MockerFixture) -> None:
    """測試所有商品回傳空資料時拋出 ValueError。"""
    mocker.patch(
        "tw_crawler.gold_price.fetch_gold_data",
        return_value=pd.DataFrame(),
    )

    with pytest.raises(ValueError, match="無法取得任何黃金價格資料"):
        gold_price.gold_price_crawler("2026-03-18")
