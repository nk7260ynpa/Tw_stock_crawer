import logging

from fastapi import FastAPI

import tw_crawler

log_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log_handler = logging.FileHandler("crawler.log")
log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

app = FastAPI()

@app.post("/")
def craw_data(name, date):
    crawlertype = name+"_crawler"
    crawler = getattr(tw_crawler, crawlertype)  # Dynamically get the class or method
    logger.info(f"Starting crawler: {crawlertype} for date: {date}")
    df = crawler(date)
    logger.info(f"Crawler {crawlertype} completed for date: {date}, data shape: {df.shape}")
    return {"data": df.to_json()}