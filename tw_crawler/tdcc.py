"""TDCC 集保戶股權分散表爬蟲模組。

提供台灣集中保管結算所(TDCC)每週股權分散表資料爬取與處理功能。
注意：TDCC API 僅提供最新一期資料，不支援指定日期查詢。
"""

import logging

import pandas as pd
import requests

logger = logging.getLogger(__name__)


def en_columns() -> list[str]:
    """回傳 TDCC 爬蟲的英文欄位名稱列表。

    Returns:
        list[str]: TDCC 英文欄位名稱列表。
    """
    return [
        "SecurityCode",
        "HoldingLevel",
        "Holders",
        "Shares",
        "Percentage",
    ]


def zh2en_columns() -> dict[str, str]:
    """回傳中文欄位名稱對應英文欄位名稱的字典。

    Returns:
        dict[str, str]: 中英文欄位名稱對照字典。
    """
    return {
        "證券代號": "SecurityCode",
        "持股分級": "HoldingLevel",
        "人數": "Holders",
        "股數": "Shares",
        "占集保庫存數比例%": "Percentage",
    }


def remove_comma(x: str) -> str:
    """移除字串中的逗號。

    Args:
        x: 含有逗號的字串。

    Returns:
        移除逗號後的字串。
    """
    return x.replace(",", "")


def post_process(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """將 TDCC 原始資料表做欄位轉換與資料清洗。

    Args:
        df: 從 TDCC 網站爬取的原始 DataFrame（已移除資料日期欄位）。
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        處理後的 DataFrame。
    """
    df = df.rename(columns=zh2en_columns())
    df["Date"] = date
    df["Date"] = pd.to_datetime(df["Date"])
    df["SecurityCode"] = df["SecurityCode"].astype(str).str.strip()
    df["HoldingLevel"] = df["HoldingLevel"].astype(int)
    df["Holders"] = (
        df["Holders"].astype(str).map(remove_comma).astype(int)
    )
    df["Shares"] = (
        df["Shares"].astype(str).map(remove_comma).astype(int)
    )
    df["Percentage"] = df["Percentage"].astype(float)

    df = df[["Date"] + [col for col in df.columns if col != "Date"]]
    return df


def gen_empty_date_df() -> pd.DataFrame:
    """產生 TDCC 無資料時的空 DataFrame。

    Returns:
        具有正確欄位的空 DataFrame。
    """
    df = pd.DataFrame(columns=en_columns())
    df.insert(0, "Date", pd.NaT)
    return df


def fetch_tdcc_data() -> list[dict]:
    """從 TDCC 開放資料平台取得最新的股權分散表資料。

    注意：此 API 不接受日期參數，僅回傳最新一期資料。

    Returns:
        TDCC 回傳的 JSON 陣列。
    """
    url = "https://openapi.tdcc.com.tw/v1/opendata/1-5"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def parse_tdcc_data(response: list[dict]) -> pd.DataFrame:
    """將 TDCC 回傳的 JSON 解析為 DataFrame。

    Args:
        response: TDCC 回傳的 JSON 陣列。

    Returns:
        解析並處理後的 DataFrame。
    """
    if not response:
        return gen_empty_date_df()

    df = pd.DataFrame(response)

    # 提取資料日期（格式 YYYYMMDD），轉為 YYYY-MM-DD
    data_date_raw = str(df["資料日期"].iloc[0]).strip()
    data_date = (
        f"{data_date_raw[:4]}-{data_date_raw[4:6]}-{data_date_raw[6:8]}"
    )

    # 移除原始的資料日期欄位，由 post_process 統一加入 Date
    df = df.drop(columns=["資料日期"])
    df = post_process(df, data_date)
    return df


def tdcc_crawler(date: str) -> pd.DataFrame:
    """爬取 TDCC 集保戶股權分散表資料。

    注意：TDCC API 僅提供最新一期資料，date 參數不影響查詢結果，
    回傳的 Date 欄位為資料本身的日期。

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'（此參數不影響查詢結果）。

    Returns:
        處理後的股權分散表資料 DataFrame。
    """
    logger.info("Starting Request data from TDCC")
    response = fetch_tdcc_data()
    df = parse_tdcc_data(response)
    return df
