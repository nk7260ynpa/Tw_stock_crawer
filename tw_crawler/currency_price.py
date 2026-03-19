"""國際匯率價格爬蟲模組。

提供 USDTWD（美元兌台幣）與 JPYTWD（日圓兌台幣）匯率爬取功能。
使用 yfinance 套件取得 Yahoo Finance 的匯率資料。
JPYTWD 若無直接 ticker，會從 TWD=X / JPY=X 交叉計算。
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# 匯率 ticker 對照表
CURRENCY_TICKERS = {
    "USDTWD": "TWD=X",
    "JPYTWD": "JPYTWD=X",
}

# 當直接 ticker 無資料時的 fallback 計算方式
FALLBACK_TICKERS = {
    "JPYTWD": {
        "numerator": "TWD=X",    # 1 USD = ? TWD
        "denominator": "JPY=X",  # 1 USD = ? JPY
    },
}


def fetch_currency_data(
    ticker: str,
    date: str,
) -> pd.DataFrame:
    """從 Yahoo Finance 取得指定日期的匯率資料。

    使用 yfinance 下載指定 ticker 在目標日期前後的歷史資料，
    以確保能取得最近的交易日資料。

    Args:
        ticker: Yahoo Finance ticker symbol（如 'TWD=X'）。
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        包含 OHLCV 資料的 DataFrame。
    """
    target_date = datetime.strptime(date, "%Y-%m-%d")
    # 往前多抓 7 天，確保即使遇到假日也能取得最近的交易日資料
    start_date = (target_date - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

    logger.debug(
        "Fetching %s data from %s to %s", ticker, start_date, end_date
    )
    currency = yf.Ticker(ticker)
    df = currency.history(start=start_date, end=end_date)
    return df


def parse_currency_data(
    df: pd.DataFrame,
    product: str,
    date: str,
) -> dict | None:
    """將 yfinance 回傳的 DataFrame 解析為匯率價格字典。

    從歷史資料中篩選出不超過目標日期的最新一筆交易日資料。
    若找不到資料則回傳 None。匯率資料的 Volume 可能為 0。

    Args:
        df: yfinance 回傳的歷史資料 DataFrame。
        product: 產品名稱（如 'USDTWD' 或 'JPYTWD'）。
        date: 查詢日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        包含匯率價格資訊的字典，若無資料則回傳 None。
    """
    if df.empty:
        logger.warning("No data returned for %s", product)
        return None

    target_date = pd.Timestamp(date).tz_localize(None)

    # 移除時區資訊以便比較
    df_clean = df.copy()
    df_clean.index = df_clean.index.tz_localize(None)

    # 篩選不超過目標日期的資料
    mask = df_clean.index <= target_date
    if not mask.any():
        logger.warning(
            "No trading data found for %s on or before %s",
            product,
            date,
        )
        return None

    latest = df_clean.loc[mask].iloc[-1]
    trade_date = df_clean.loc[mask].index[-1].strftime("%Y-%m-%d")

    # 匯率的 Volume 可能為 0，預設為 0
    volume = int(latest["Volume"]) if "Volume" in latest else 0

    return {
        "product": product,
        "date": trade_date,
        "open": round(float(latest["Open"]), 4),
        "high": round(float(latest["High"]), 4),
        "low": round(float(latest["Low"]), 4),
        "close": round(float(latest["Close"]), 4),
        "volume": volume,
    }


def _fetch_fallback_jpytwd(date: str) -> dict | None:
    """透過 TWD=X / JPY=X 交叉計算 JPYTWD 匯率。

    當 JPYTWD=X 無法直接取得資料時，使用以下公式計算：
    1 JPY = ? TWD = (1 USD / ? TWD) / (1 USD / ? JPY) = TWD=X / JPY=X

    Args:
        date: 查詢日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        包含 JPYTWD 匯率資訊的字典，若無法計算則回傳 None。
    """
    fallback = FALLBACK_TICKERS["JPYTWD"]
    logger.info(
        "Attempting JPYTWD fallback calculation using %s / %s",
        fallback["numerator"],
        fallback["denominator"],
    )

    try:
        df_twd = fetch_currency_data(fallback["numerator"], date)
        twd_data = parse_currency_data(df_twd, "TWD", date)
        if twd_data is None:
            logger.warning("Failed to get TWD=X data for fallback")
            return None

        df_jpy = fetch_currency_data(fallback["denominator"], date)
        jpy_data = parse_currency_data(df_jpy, "JPY", date)
        if jpy_data is None:
            logger.warning("Failed to get JPY=X data for fallback")
            return None

        # 1 JPY = (1 USD in TWD) / (1 USD in JPY) = TWD=X / JPY=X
        open_rate = twd_data["open"] / jpy_data["open"]
        high_rate = twd_data["high"] / jpy_data["low"]
        low_rate = twd_data["low"] / jpy_data["high"]
        close_rate = twd_data["close"] / jpy_data["close"]

        result = {
            "product": "JPYTWD",
            "date": twd_data["date"],
            "open": round(open_rate, 4),
            "high": round(high_rate, 4),
            "low": round(low_rate, 4),
            "close": round(close_rate, 4),
            "volume": 0,
        }

        logger.info(
            "JPYTWD fallback calculated: close=%.4f",
            result["close"],
        )
        return result

    except Exception as e:
        logger.error("JPYTWD fallback calculation failed: %s", e)
        return None


def currency_price_crawler(date: str) -> list[dict]:
    """爬取指定日期的匯率價格（USDTWD 與 JPYTWD）。

    依序從 Yahoo Finance 取得美元兌台幣與日圓兌台幣的匯率資料。
    JPYTWD 若無直接 ticker 資料，會自動使用 TWD=X / JPY=X 交叉計算。

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        包含匯率價格資訊的字典列表。每筆資料包含：
        product, date, open, high, low, close, volume。
        若某商品無資料則不包含在列表中。

    Raises:
        ValueError: 當所有匯率均無法取得資料時。
    """
    logger.info("Starting currency price crawler for date: %s", date)
    results = []

    for product, ticker in CURRENCY_TICKERS.items():
        try:
            logger.info("Fetching %s (%s) data", product, ticker)
            df = fetch_currency_data(ticker, date)
            parsed = parse_currency_data(df, product, date)

            if parsed is not None:
                results.append(parsed)
                logger.info(
                    "%s rate fetched: close=%.4f",
                    product,
                    parsed["close"],
                )
            elif product in FALLBACK_TICKERS:
                # 直接 ticker 無資料，嘗試 fallback 計算
                logger.info(
                    "Direct ticker failed for %s, trying fallback",
                    product,
                )
                fallback_result = _fetch_fallback_jpytwd(date)
                if fallback_result is not None:
                    results.append(fallback_result)
                else:
                    logger.warning(
                        "Fallback also failed for %s on %s",
                        product,
                        date,
                    )
            else:
                logger.warning(
                    "No data available for %s on %s", product, date
                )
        except Exception as e:
            logger.error("Failed to fetch %s data: %s", product, e)
            # 若有 fallback 且直接取得時出錯，也嘗試 fallback
            if product in FALLBACK_TICKERS:
                logger.info(
                    "Direct ticker errored for %s, trying fallback",
                    product,
                )
                fallback_result = _fetch_fallback_jpytwd(date)
                if fallback_result is not None:
                    results.append(fallback_result)

    if not results:
        raise ValueError(
            f"無法取得任何匯率資料（查詢日期：{date}）"
        )

    logger.info(
        "Currency price crawler completed, products: %d", len(results)
    )
    return results
