"""FastAPI 股價資料 Server。

提供單一 GET endpoint，自動爬取當天所有股價資料（上市、上櫃、期貨、三大法人、融資融券）。
"""

import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import FastAPI

import tw_crawler

log_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log_handler = logging.FileHandler("crawler.log")
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
    logger.info(f"Starting all crawlers for date: {today}")

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
                logger.info(f"Crawler {name} completed, rows: {len(df)}")
            except Exception as e:
                logger.error(f"Crawler {name} failed: {e}")
                results[name] = {"error": str(e)}

    return {"date": today, "data": results}
