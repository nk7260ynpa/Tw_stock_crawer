"""FastAPI 股價資料 Server。

提供多個 GET endpoint，可分別或同時爬取股價資料
（上市、上櫃、期貨、三大法人、融資融券）。
"""

import datetime
import logging
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import TimedRotatingFileHandler
from typing import Annotated

from fastapi import FastAPI, Query

import tw_crawler

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log_handler = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, "crawler.log"),
    when="midnight",
    backupCount=30,
)
log_handler.suffix = "%Y-%m-%d"
log_handler.namer = lambda name: name.replace(
    "crawler.log.", "crawler."
) + ".log"
log_handler.setFormatter(log_formatter)

# 設定 tw_crawler package logger，確保所有子模組日誌都寫入檔案
tw_crawler_logger = logging.getLogger("tw_crawler")
tw_crawler_logger.setLevel(logging.DEBUG)
tw_crawler_logger.addHandler(log_handler)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

app = FastAPI()

# 共用 Query 參數型別（Annotated）：沿用原本的 str／int 內層標註（搭配 None 預設），
# 消除各 endpoint 重複的 date／hours Query 樣板，且 OpenAPI schema 與重構前完全一致。
DateParam = Annotated[
    str, Query(description="查詢日期，格式為 YYYY-MM-DD，預設為當天")
]
HoursParam = Annotated[
    int,
    Query(description="抓取過去幾小時內的新聞（與 date 擇一使用）", ge=1, le=72),
]

CRAWLERS = {
    "twse": tw_crawler.twse_crawler,
    "tpex": tw_crawler.tpex_crawler,
    "taifex": tw_crawler.taifex_crawler,
    "faoi": tw_crawler.faoi_crawler,
    "mgts": tw_crawler.mgts_crawler,
    "tdcc": tw_crawler.tdcc_crawler,
}


def _get_date(date: str | None) -> str:
    """取得查詢日期，若未指定則使用當天日期。

    Args:
        date: 日期字串或 None。

    Returns:
        格式為 'YYYY-MM-DD' 的日期字串。
    """
    if date is None:
        return datetime.date.today().strftime("%Y-%m-%d")
    return date


def _run_crawler(name: str, date: str) -> dict:
    """執行單一爬蟲並回傳結果。

    Args:
        name: 爬蟲名稱。
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        包含日期與爬蟲資料的字典。
    """
    logger.info("Starting crawler %s for date: %s", name, date)
    try:
        df = CRAWLERS[name](date)
        logger.info(
            "Crawler %s completed, rows: %d", name, len(df)
        )
        return {"date": date, "data": df.to_dict(orient="records")}
    except Exception as e:
        logger.error("Crawler %s failed: %s", name, e)
        return {"date": date, "error": str(e)}


@app.get("/")
def crawl_all(
    date: str = Query(
        default=None,
        description="查詢日期，格式為 YYYY-MM-DD，預設為當天",
    ),
) -> dict:
    """爬取指定日期所有股價資料並回傳 JSON。"""
    date = _get_date(date)
    logger.info("Starting all crawlers for date: %s", date)

    results = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        future_to_name = {
            executor.submit(crawler, date): name
            for name, crawler in CRAWLERS.items()
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                df = future.result()
                results[name] = df.to_dict(orient="records")
                logger.info(
                    "Crawler %s completed, rows: %d", name, len(df)
                )
            except Exception as e:
                logger.error("Crawler %s failed: %s", name, e)
                results[name] = {"error": str(e)}

    return {"date": date, "data": results}


# === 路由工廠 ===
# 以「設定表 + 工廠 + 迴圈 add_api_route」收斂同構 endpoint，消除重複的 date／hours
# Query、try/except 與 logger 樣板。新聞／商品工廠在「請求當下」才以 getattr 取得對應
# crawler（不在註冊時捕捉函式參考），確保測試的 mocker.patch 仍可生效。

# --- Market（上市／上櫃／期貨／三大法人／融資融券／集保）---
# 經模組層 CRAWLERS dict 呼叫（供 mocker.patch("server.CRAWLERS")）。
_MARKET_ENDPOINTS = [
    ("twse", "爬取指定日期的上市股票資料。"),
    ("tpex", "爬取指定日期的上櫃股票資料。"),
    ("taifex", "爬取指定日期的期貨資料。"),
    ("faoi", "爬取指定日期的三大法人買賣超資料。"),
    ("mgts", "爬取指定日期的融資融券資料。"),
    ("tdcc", "爬取集保戶股權分散表資料（僅回傳最新一期）。"),
]


def _make_market_endpoint(name: str, description: str) -> Callable:
    """建立單一股市爬蟲 endpoint（經 CRAWLERS 呼叫）。"""

    def endpoint(date: DateParam = None) -> dict:
        return _run_crawler(name, _get_date(date))

    endpoint.__name__ = f"crawl_{name}"
    endpoint.__doc__ = description
    return endpoint


# --- News（CTEE／鉅亨網／PTT／聯合新聞網經濟日報），支援 date／hours 雙模式 ---
# (key, log 標籤, 描述首行, 單位詞)
_NEWS_ENDPOINTS = [
    ("ctee", "CTEE", "爬取 CTEE 工商時報股市新聞。", "新聞"),
    ("cnyes", "CNYES", "爬取鉅亨網台股新聞。", "新聞"),
    ("ptt", "PTT", "爬取 PTT 股版文章。", "文章"),
    ("moneyudn", "MoneyUDN", "爬取聯合新聞網經濟日報台股新聞。", "新聞"),
]


def _make_news_endpoint(
    key: str, label: str, head: str, unit: str
) -> Callable:
    """建立單一新聞爬蟲 endpoint（date／hours 雙模式）。"""

    def endpoint(
        date: DateParam = None, hours: HoursParam = None
    ) -> dict:
        date = _get_date(date)
        mode = f"hours={hours}" if hours else f"date={date}"
        logger.info("Starting %s news crawler: %s", label, mode)
        try:
            crawler = getattr(tw_crawler, f"{key}_news_crawler")
            df = crawler(date, hours=hours)
            logger.info(
                "%s news crawler completed, articles: %d", label, len(df)
            )
            result = {"date": date, "data": df.to_dict(orient="records")}
            if hours is not None:
                result["hours"] = hours
            return result
        except Exception as e:
            logger.error("%s news crawler failed: %s", label, e)
            return {"date": date, "error": str(e)}

    endpoint.__name__ = f"crawl_{key}_news"
    endpoint.__doc__ = (
        f"{head}\n\n"
        "支援兩種模式：\n"
        f"- date 模式：抓取指定日期的全部{unit}\n"
        f"- hours 模式：抓取過去 N 小時內的{unit}\n"
        "hours 優先於 date，同時指定時以 hours 為準。"
    )
    return endpoint


# --- Commodity（原油／黃金／比特幣／匯率／指數），yfinance，回傳 list[dict] ---
# (key, 描述)；回傳直接放 list，不做 DataFrame 轉換。
_COMMODITY_ENDPOINTS = [
    ("oil", "爬取國際原油價格（WTI 西德州原油 + Brent 布蘭特原油）。\n\n"
            "使用 yfinance 從 Yahoo Finance 取得原油期貨價格資料。\n"
            "若查詢日期為非交易日，會回傳最近一個交易日的資料。"),
    ("gold", "爬取國際黃金價格（COMEX Gold Futures）。\n\n"
             "使用 yfinance 從 Yahoo Finance 取得黃金期貨價格資料。\n"
             "若查詢日期為非交易日，會回傳最近一個交易日的資料。"),
    ("bitcoin", "爬取比特幣價格（BTC-USD）。\n\n"
                "使用 yfinance 從 Yahoo Finance 取得比特幣價格資料。\n"
                "若查詢日期為非交易日，會回傳最近一個交易日的資料。"),
    ("currency", "爬取國際匯率（USDTWD 美元兌台幣 + JPYTWD 日圓兌台幣）。\n\n"
                 "使用 yfinance 從 Yahoo Finance 取得匯率資料。\n"
                 "若查詢日期為非交易日，會回傳最近一個交易日的資料。\n"
                 "JPYTWD 若無直接 ticker，會自動從 TWD=X / JPY=X 交叉計算。"),
    ("indices", "爬取國際股市指數（道瓊工業指數 + 納斯達克綜合指數）。\n\n"
                "使用 yfinance 從 Yahoo Finance 取得股市指數價格資料。\n"
                "若查詢日期為非交易日，會回傳最近一個交易日的資料。"),
]


def _make_commodity_endpoint(key: str, description: str) -> Callable:
    """建立單一國際商品爬蟲 endpoint（回傳 list[dict]）。"""

    def endpoint(date: DateParam = None) -> dict:
        date = _get_date(date)
        logger.info("Starting %s price crawler for date: %s", key, date)
        try:
            crawler = getattr(tw_crawler, f"{key}_price_crawler")
            result = crawler(date)
            logger.info(
                "%s price crawler completed, products: %d",
                key.capitalize(), len(result),
            )
            return {"date": date, "data": result}
        except Exception as e:
            logger.error(
                "%s price crawler failed: %s", key.capitalize(), e
            )
            return {"date": date, "error": str(e)}

    endpoint.__name__ = f"crawl_{key}_price"
    endpoint.__doc__ = description
    return endpoint


# 迴圈註冊三組同構 endpoint。
for _name, _desc in _MARKET_ENDPOINTS:
    app.add_api_route(
        f"/{_name}",
        _make_market_endpoint(_name, _desc),
        methods=["GET"],
    )

for _key, _label, _head, _unit in _NEWS_ENDPOINTS:
    app.add_api_route(
        f"/{_key}_news",
        _make_news_endpoint(_key, _label, _head, _unit),
        methods=["GET"],
    )

for _key, _desc in _COMMODITY_ENDPOINTS:
    app.add_api_route(
        f"/{_key}_price",
        _make_commodity_endpoint(_key, _desc),
        methods=["GET"],
    )


@app.get("/company_info")
def crawl_company_info() -> dict:
    """爬取上市與上櫃公司基本資料及產業對照表。

    從 TWSE 與 TPEX OpenAPI 取得公司基本資料，
    合併為統一格式並建立產業代碼對照表。
    此端點不需要 date 參數，因為公司資料不依日期變動。
    """
    logger.info("Starting company info crawler")
    try:
        result = tw_crawler.company_info_crawler()
        logger.info(
            "Company info crawler completed, "
            "TWSE: %d, TPEX: %d",
            result["twse_count"],
            result["tpex_count"],
        )
        return {"data": result}
    except Exception as e:
        logger.error("Company info crawler failed: %s", e)
        return {"error": str(e)}
