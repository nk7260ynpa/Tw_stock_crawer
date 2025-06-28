import requests

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