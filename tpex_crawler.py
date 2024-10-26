import requests
import pandas as pd



def tpex_headers():
    """
    Return headers for TPEX crawler

    Returns:
        dict: headers for TPEX crawler

    Examples:
        >>> tpex_headers()
    """
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7',
        'Connection': 'keep-alive',
        'Host': 'www.tpex.org.tw',
        'Referer': 'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430.php?l=zh-tw',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
         }
    return headers

def tpex_crawler(date):
    url = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={date.replace("-", "/")}&se=AL"
    print(url)
    result = requests.get(url, headers=tpex_headers())
    #result = result.json()
    # if result["stat"] == "OK":
    #     target_table = result["tables"][8]
    #     df = pd.DataFrame(columns=target_table["fields"], data=target_table["data"])
    #     df = post_process(df, date)
    # else:
    #     df = pd.DataFrame(columns=en_columns())
    return result
