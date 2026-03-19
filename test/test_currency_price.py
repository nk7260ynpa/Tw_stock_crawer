"""國際匯率價格爬蟲測試模組。"""

import pandas as pd
import pytest
from pytest_mock import MockerFixture

import tw_crawler.currency_price as currency_price


def _make_usdtwd_df() -> pd.DataFrame:
    """建立模擬的 USDTWD 匯率歷史資料 DataFrame。"""
    index = pd.DatetimeIndex(
        ["2026-03-16", "2026-03-17", "2026-03-18"],
        tz="UTC",
    )
    return pd.DataFrame(
        {
            "Open": [32.5100, 32.5500, 32.6000],
            "High": [32.6000, 32.6200, 32.6500],
            "Low": [32.4800, 32.5200, 32.5600],
            "Close": [32.5500, 32.6000, 32.6200],
            "Volume": [0, 0, 0],
            "Dividends": [0.0, 0.0, 0.0],
            "Stock Splits": [0.0, 0.0, 0.0],
        },
        index=index,
    )


def _make_jpytwd_df() -> pd.DataFrame:
    """建立模擬的 JPYTWD=X 匯率歷史資料 DataFrame。"""
    index = pd.DatetimeIndex(
        ["2026-03-16", "2026-03-17", "2026-03-18"],
        tz="UTC",
    )
    return pd.DataFrame(
        {
            "Open": [0.2170, 0.2175, 0.2180],
            "High": [0.2180, 0.2185, 0.2190],
            "Low": [0.2165, 0.2170, 0.2175],
            "Close": [0.2175, 0.2180, 0.2185],
            "Volume": [0, 0, 0],
            "Dividends": [0.0, 0.0, 0.0],
            "Stock Splits": [0.0, 0.0, 0.0],
        },
        index=index,
    )


def _make_jpyusd_df() -> pd.DataFrame:
    """建立模擬的 JPY=X（1 USD = ? JPY）歷史資料 DataFrame。"""
    index = pd.DatetimeIndex(
        ["2026-03-16", "2026-03-17", "2026-03-18"],
        tz="UTC",
    )
    return pd.DataFrame(
        {
            "Open": [149.5000, 149.8000, 150.0000],
            "High": [150.0000, 150.2000, 150.5000],
            "Low": [149.2000, 149.5000, 149.8000],
            "Close": [149.8000, 150.0000, 150.2000],
            "Volume": [0, 0, 0],
            "Dividends": [0.0, 0.0, 0.0],
            "Stock Splits": [0.0, 0.0, 0.0],
        },
        index=index,
    )


def test_currency_tickers() -> None:
    """測試匯率 ticker 對照表包含 USDTWD 和 JPYTWD。"""
    assert "USDTWD" in currency_price.CURRENCY_TICKERS
    assert "JPYTWD" in currency_price.CURRENCY_TICKERS
    assert currency_price.CURRENCY_TICKERS["USDTWD"] == "TWD=X"
    assert currency_price.CURRENCY_TICKERS["JPYTWD"] == "JPYTWD=X"


def test_fallback_tickers() -> None:
    """測試 JPYTWD fallback ticker 設定。"""
    assert "JPYTWD" in currency_price.FALLBACK_TICKERS
    fb = currency_price.FALLBACK_TICKERS["JPYTWD"]
    assert fb["numerator"] == "TWD=X"
    assert fb["denominator"] == "JPY=X"


def test_fetch_currency_data(mocker: MockerFixture) -> None:
    """測試 fetch_currency_data 正確呼叫 yfinance API。"""
    mock_df = _make_usdtwd_df()
    mock_ticker = mocker.Mock()
    mock_ticker.history.return_value = mock_df
    mocker.patch(
        "tw_crawler.currency_price.yf.Ticker",
        return_value=mock_ticker,
    )

    result = currency_price.fetch_currency_data("TWD=X", "2026-03-18")

    currency_price.yf.Ticker.assert_called_once_with("TWD=X")
    mock_ticker.history.assert_called_once_with(
        start="2026-03-11", end="2026-03-19"
    )
    pd.testing.assert_frame_equal(result, mock_df)


def test_parse_currency_data_normal() -> None:
    """測試正常解析匯率資料。"""
    df = _make_usdtwd_df()
    result = currency_price.parse_currency_data(
        df, "USDTWD", "2026-03-18"
    )

    assert result is not None
    assert result["product"] == "USDTWD"
    assert result["date"] == "2026-03-18"
    assert result["open"] == 32.6000
    assert result["high"] == 32.6500
    assert result["low"] == 32.5600
    assert result["close"] == 32.6200
    assert result["volume"] == 0


def test_parse_currency_data_holiday() -> None:
    """測試查詢假日時回傳最近交易日的資料。"""
    df = _make_usdtwd_df()
    result = currency_price.parse_currency_data(
        df, "USDTWD", "2026-03-19"
    )

    assert result is not None
    assert result["date"] == "2026-03-18"
    assert result["close"] == 32.6200


def test_parse_currency_data_specific_date() -> None:
    """測試查詢特定日期，只回傳該日或之前的資料。"""
    df = _make_usdtwd_df()
    result = currency_price.parse_currency_data(
        df, "USDTWD", "2026-03-17"
    )

    assert result is not None
    assert result["date"] == "2026-03-17"
    assert result["close"] == 32.6000


def test_parse_currency_data_empty() -> None:
    """測試空的 DataFrame 回傳 None。"""
    df = pd.DataFrame()
    result = currency_price.parse_currency_data(
        df, "USDTWD", "2026-03-18"
    )

    assert result is None


def test_parse_currency_data_no_match() -> None:
    """測試查詢日期早於所有資料時回傳 None。"""
    df = _make_usdtwd_df()
    result = currency_price.parse_currency_data(
        df, "USDTWD", "2026-03-10"
    )

    assert result is None


def test_parse_currency_data_round_4_decimals() -> None:
    """測試匯率數值 round 到 4 位小數。"""
    df = _make_jpytwd_df()
    result = currency_price.parse_currency_data(
        df, "JPYTWD", "2026-03-18"
    )

    assert result is not None
    # 確認小數位數不超過 4
    for key in ["open", "high", "low", "close"]:
        str_val = str(result[key])
        if "." in str_val:
            decimals = len(str_val.split(".")[1])
            assert decimals <= 4


def test_currency_price_crawler_both_success(
    mocker: MockerFixture,
) -> None:
    """測試 USDTWD 和 JPYTWD 都成功取得資料。"""
    usdtwd_df = _make_usdtwd_df()
    jpytwd_df = _make_jpytwd_df()

    call_count = 0

    def _side_effect(ticker: str, date: str) -> pd.DataFrame:
        nonlocal call_count
        call_count += 1
        if ticker == "TWD=X":
            return usdtwd_df
        elif ticker == "JPYTWD=X":
            return jpytwd_df
        return pd.DataFrame()

    mocker.patch(
        "tw_crawler.currency_price.fetch_currency_data",
        side_effect=_side_effect,
    )

    result = currency_price.currency_price_crawler("2026-03-18")

    assert len(result) == 2
    products = [r["product"] for r in result]
    assert "USDTWD" in products
    assert "JPYTWD" in products

    usdtwd = next(r for r in result if r["product"] == "USDTWD")
    assert usdtwd["close"] == 32.6200
    assert usdtwd["volume"] == 0


def test_currency_price_crawler_jpytwd_fallback(
    mocker: MockerFixture,
) -> None:
    """測試 JPYTWD=X 無資料時使用 fallback 計算。"""
    usdtwd_df = _make_usdtwd_df()
    jpyusd_df = _make_jpyusd_df()

    def _side_effect(ticker: str, date: str) -> pd.DataFrame:
        if ticker == "TWD=X":
            return usdtwd_df
        elif ticker == "JPYTWD=X":
            return pd.DataFrame()  # JPYTWD=X 無資料
        elif ticker == "JPY=X":
            return jpyusd_df
        return pd.DataFrame()

    mocker.patch(
        "tw_crawler.currency_price.fetch_currency_data",
        side_effect=_side_effect,
    )

    result = currency_price.currency_price_crawler("2026-03-18")

    assert len(result) == 2
    products = [r["product"] for r in result]
    assert "USDTWD" in products
    assert "JPYTWD" in products

    jpytwd = next(r for r in result if r["product"] == "JPYTWD")
    # TWD=X close=32.6200, JPY=X close=150.2000
    # JPYTWD = 32.6200 / 150.2000 = 0.2172...
    expected_close = round(32.6200 / 150.2000, 4)
    assert jpytwd["close"] == expected_close
    assert jpytwd["volume"] == 0


def test_currency_price_crawler_jpytwd_fallback_on_error(
    mocker: MockerFixture,
) -> None:
    """測試 JPYTWD=X 拋出例外時使用 fallback 計算。"""
    usdtwd_df = _make_usdtwd_df()
    jpyusd_df = _make_jpyusd_df()

    call_count = 0

    def _side_effect(ticker: str, date: str) -> pd.DataFrame:
        nonlocal call_count
        call_count += 1
        if ticker == "TWD=X":
            return usdtwd_df
        elif ticker == "JPYTWD=X":
            raise ConnectionError("Network error")
        elif ticker == "JPY=X":
            return jpyusd_df
        return pd.DataFrame()

    mocker.patch(
        "tw_crawler.currency_price.fetch_currency_data",
        side_effect=_side_effect,
    )

    result = currency_price.currency_price_crawler("2026-03-18")

    assert len(result) == 2
    products = [r["product"] for r in result]
    assert "JPYTWD" in products


def test_currency_price_crawler_all_fail(
    mocker: MockerFixture,
) -> None:
    """測試所有匯率都失敗時拋出 ValueError。"""
    mocker.patch(
        "tw_crawler.currency_price.fetch_currency_data",
        side_effect=ConnectionError("Network error"),
    )

    with pytest.raises(ValueError, match="無法取得任何匯率資料"):
        currency_price.currency_price_crawler("2026-03-18")


def test_currency_price_crawler_all_empty(
    mocker: MockerFixture,
) -> None:
    """測試所有匯率回傳空資料時拋出 ValueError。"""
    mocker.patch(
        "tw_crawler.currency_price.fetch_currency_data",
        return_value=pd.DataFrame(),
    )

    with pytest.raises(ValueError, match="無法取得任何匯率資料"):
        currency_price.currency_price_crawler("2026-03-18")


def test_fetch_fallback_jpytwd_success(
    mocker: MockerFixture,
) -> None:
    """測試 fallback JPYTWD 計算成功。"""
    usdtwd_df = _make_usdtwd_df()
    jpyusd_df = _make_jpyusd_df()

    def _side_effect(ticker: str, date: str) -> pd.DataFrame:
        if ticker == "TWD=X":
            return usdtwd_df
        elif ticker == "JPY=X":
            return jpyusd_df
        return pd.DataFrame()

    mocker.patch(
        "tw_crawler.currency_price.fetch_currency_data",
        side_effect=_side_effect,
    )

    result = currency_price._fetch_fallback_jpytwd("2026-03-18")

    assert result is not None
    assert result["product"] == "JPYTWD"
    assert result["date"] == "2026-03-18"
    assert result["volume"] == 0


def test_fetch_fallback_jpytwd_twd_fail(
    mocker: MockerFixture,
) -> None:
    """測試 fallback 計算中 TWD=X 無資料時回傳 None。"""
    mocker.patch(
        "tw_crawler.currency_price.fetch_currency_data",
        return_value=pd.DataFrame(),
    )

    result = currency_price._fetch_fallback_jpytwd("2026-03-18")

    assert result is None


def test_fetch_fallback_jpytwd_jpy_fail(
    mocker: MockerFixture,
) -> None:
    """測試 fallback 計算中 JPY=X 無資料時回傳 None。"""
    usdtwd_df = _make_usdtwd_df()

    def _side_effect(ticker: str, date: str) -> pd.DataFrame:
        if ticker == "TWD=X":
            return usdtwd_df
        return pd.DataFrame()

    mocker.patch(
        "tw_crawler.currency_price.fetch_currency_data",
        side_effect=_side_effect,
    )

    result = currency_price._fetch_fallback_jpytwd("2026-03-18")

    assert result is None
