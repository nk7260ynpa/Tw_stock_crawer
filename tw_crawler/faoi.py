import requests
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def parse_faoi_data(response, date):
    if response["stat"] == "OK":
        df = pd.DataFrame(columns=response["fields"], data=response["data"])
        df = post_process(df, date)
    else:
        df = gen_empty_date_df()
    return df

def fetch_faoi_data(date):
    url = f'https://www.twse.com.tw/rwd/zh/fund/T86?date={date.replace("-", "")}&selectType=ALL&response=json'
    response = requests.get(url)
    return response.json()

def faoi_crawler(date):
    logger.info(f"Starting Request data from Foreign and Other Investors")
    response = fetch_faoi_data(date)
    df = parse_faoi_data(response, date)
    return df