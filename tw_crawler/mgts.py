import requests
import logging

import pandas as pd

logger = logging.getLogger(__name__)

def parse_mgts_data(response, date):
    """
    Parse the JSON response from the MGTS website into a DataFrame.

    Args:
        data (dict): The JSON response from the MGTS website.

    Returns:
        pd.DataFrame: The parsed DataFrame.

    Examples:
        >>> parse_mgts_data(data)
    """
    if response["stat"] == "OK":
        df = pd.DataFrame(columns=response["tables"][1]["fields"], data=response["tables"][1]["data"])
        df = post_process(df, date)
    else:
        df = gen_empty_date_df()
    return df

def fetch_mgts_data(date):
    """
    Crawl the MGTS website for stock data on a given date and process it.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: The processed DataFrame containing stock data.

    Examples:
        >>> MGTS_crawler("2022-02-18")
    """
    url = f'https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={date.replace("-", "")}&selectType=ALL&response=json'
    response = requests.get(url)
    return response.json()

def mgts_crawler(date):
    """
    Crawl the MGTS website for stock data on a given date and process it.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: The processed DataFrame containing stock data.

    Examples:
        >>> mgts_crawler("2022-02-18")
    """
    logger.info(f"Starting Request data from Foreign and Other Investors")
    response = fetch_mgts_data(date)
    df = parse_mgts_data(response, date)
    return df