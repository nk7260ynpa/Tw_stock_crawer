"""FastAPI 股價資料 Server。

提供單一 GET endpoint，自動爬取當天所有股價資料
（上市、上櫃、期貨、三大法人、融資融券）。
"""

import datetime
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import TimedRotatingFileHandler

from fastapi import FastAPI

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


@app.get("/")
def crawl_all() -> dict:
    """爬取當天所有股價資料並回傳 JSON。"""
    today = datetime.date.today().strftime("%Y-%m-%d")
    logger.info("Starting all crawlers for date: %s", today)

    results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_name = {
            executor.submit(crawler, today): name
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

    return {"date": today, "data": results}
