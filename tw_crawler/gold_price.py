"""國際黃金價格爬蟲模組。

提供 COMEX 黃金期貨價格爬取功能。
使用 yfinance 套件取得 Yahoo Finance 的黃金期貨資料。
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# 黃金期貨 ticker 對照表
GOLD_TICKERS = {
    "Gold": "GC=F",
}


def fetch_gold_data(
    ticker: str,
    date: str,
) -> pd.DataFrame:
    """從 Yahoo Finance 取得指定日期的黃金期貨資料。

    使用 yfinance 下載指定 ticker 在目標日期前後的歷史資料，
    以確保能取得最近的交易日資料。

    Args:
        ticker: Yahoo Finance ticker symbol（如 'GC=F'）。
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
    gold = yf.Ticker(ticker)
    df = gold.history(start=start_date, end=end_date)
    return df


def parse_gold_data(
    df: pd.DataFrame,
    product: str,
    date: str,
) -> dict | None:
    """將 yfinance 回傳的 DataFrame 解析為黃金價格字典。

    從歷史資料中篩選出不超過目標日期的最新一筆交易日資料。
    若找不到資料則回傳 None。

    Args:
        df: yfinance 回傳的歷史資料 DataFrame。
        product: 產品名稱（如 'Gold'）。
        date: 查詢日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        包含黃金價格資訊的字典，若無資料則回傳 None。
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

    return {
        "product": product,
        "date": trade_date,
        "open": round(float(latest["Open"]), 2),
        "high": round(float(latest["High"]), 2),
        "low": round(float(latest["Low"]), 2),
        "close": round(float(latest["Close"]), 2),
        "volume": int(latest["Volume"]),
    }


def gold_price_crawler(date: str) -> list[dict]:
    """爬取指定日期的國際黃金價格（COMEX Gold Futures）。

    從 Yahoo Finance 取得 COMEX 黃金期貨價格資料，
    並回傳為字典列表。

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        包含黃金價格資訊的字典列表。每筆資料包含：
        product, date, open, high, low, close, volume。
        若某商品無資料則不包含在列表中。

    Raises:
        ValueError: 當所有商品均無法取得資料時。
    """
    logger.info("Starting gold price crawler for date: %s", date)
    results = []

    for product, ticker in GOLD_TICKERS.items():
        try:
            logger.info("Fetching %s (%s) data", product, ticker)
            df = fetch_gold_data(ticker, date)
            parsed = parse_gold_data(df, product, date)
            if parsed is not None:
                results.append(parsed)
                logger.info(
                    "%s price fetched: close=%.2f",
                    product,
                    parsed["close"],
                )
            else:
                logger.warning(
                    "No data available for %s on %s", product, date
                )
        except Exception as e:
            logger.error("Failed to fetch %s data: %s", product, e)

    if not results:
        raise ValueError(
            f"無法取得任何黃金價格資料（查詢日期：{date}）"
        )

    logger.info(
        "Gold price crawler completed, products: %d", len(results)
    )
    return results
