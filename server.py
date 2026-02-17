"""FastAPI 股價資料 Server。

提供多個 GET endpoint，可分別或同時爬取股價資料
（上市、上櫃、期貨、三大法人、融資融券）。
"""

import datetime
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import TimedRotatingFileHandler

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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

app = FastAPI()

CRAWLERS = {
    "twse": tw_crawler.twse_crawler,
    "tpex": tw_crawler.tpex_crawler,
    "taifex": tw_crawler.taifex_crawler,
    "faoi": tw_crawler.faoi_crawler,
    "mgts": tw_crawler.mgts_crawler,
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
    with ThreadPoolExecutor(max_workers=5) as executor:
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


@app.get("/twse")
def crawl_twse(
    date: str = Query(
        default=None,
        description="查詢日期，格式為 YYYY-MM-DD，預設為當天",
    ),
) -> dict:
    """爬取指定日期的上市股票資料。"""
    return _run_crawler("twse", _get_date(date))


@app.get("/tpex")
def crawl_tpex(
    date: str = Query(
        default=None,
        description="查詢日期，格式為 YYYY-MM-DD，預設為當天",
    ),
) -> dict:
    """爬取指定日期的上櫃股票資料。"""
    return _run_crawler("tpex", _get_date(date))


@app.get("/taifex")
def crawl_taifex(
    date: str = Query(
        default=None,
        description="查詢日期，格式為 YYYY-MM-DD，預設為當天",
    ),
) -> dict:
    """爬取指定日期的期貨資料。"""
    return _run_crawler("taifex", _get_date(date))


@app.get("/faoi")
def crawl_faoi(
    date: str = Query(
        default=None,
        description="查詢日期，格式為 YYYY-MM-DD，預設為當天",
    ),
) -> dict:
    """爬取指定日期的三大法人買賣超資料。"""
    return _run_crawler("faoi", _get_date(date))


@app.get("/mgts")
def crawl_mgts(
    date: str = Query(
        default=None,
        description="查詢日期，格式為 YYYY-MM-DD，預設為當天",
    ),
) -> dict:
    """爬取指定日期的融資融券資料。"""
    return _run_crawler("mgts", _get_date(date))
