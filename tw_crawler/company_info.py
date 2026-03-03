"""公司產業對照爬蟲模組。

提供上市(TWSE)與上櫃(TPEX)公司基本資料爬取功能，
包含公司代號、公司名稱、產業別代碼、特別股股數、普通股股數、私募股數。
同時提供產業代碼與產業名稱的對照表。
"""

import logging
import re

import pandas as pd
import requests

logger = logging.getLogger(__name__)

TWSE_API_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_API_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"

# TWSE 產業代碼對照表（上市公司）
TWSE_INDUSTRY_MAP: dict[str, str] = {
    "01": "水泥工業",
    "02": "食品工業",
    "03": "塑膠工業",
    "04": "紡織纖維",
    "05": "電機機械",
    "06": "電器電纜",
    "08": "玻璃陶瓷",
    "09": "造紙工業",
    "10": "鋼鐵工業",
    "11": "橡膠工業",
    "12": "汽車工業",
    "14": "建材營造業",
    "15": "航運業",
    "16": "觀光餐旅",
    "17": "金融保險業",
    "18": "貿易百貨業",
    "20": "其他業",
    "21": "化學工業",
    "22": "生技醫療業",
    "23": "油電燃氣業",
    "24": "半導體業",
    "25": "電腦及週邊設備業",
    "26": "光電業",
    "27": "通信網路業",
    "28": "電子零組件業",
    "29": "電子通路業",
    "30": "資訊服務業",
    "31": "其他電子業",
    "35": "綠能環保",
    "36": "數位雲端",
    "37": "運動休閒",
    "38": "居家生活",
    "91": "存託憑證",
}

# TPEX 產業代碼對照表（上櫃公司）
TPEX_INDUSTRY_MAP: dict[str, str] = {
    "01": "食品工業",
    "02": "塑膠工業",
    "03": "紡織纖維",
    "04": "電機機械",
    "05": "電器電纜",
    "06": "化學生技醫療",
    "08": "玻璃陶瓷",
    "10": "鋼鐵工業",
    "11": "橡膠工業",
    "14": "建材營造",
    "15": "航運業",
    "16": "觀光餐旅",
    "17": "金融業",
    "18": "貿易百貨",
    "20": "其他",
    "21": "化學工業",
    "22": "生技醫療業",
    "23": "油電燃氣業",
    "24": "半導體業",
    "25": "電腦及週邊設備業",
    "26": "光電業",
    "27": "通信網路業",
    "28": "電子零組件業",
    "29": "電子通路業",
    "30": "資訊服務業",
    "31": "其他電子業",
    "32": "文化創意業",
    "33": "農業科技業",
    "34": "電子商務",
    "35": "綠能環保",
    "36": "數位雲端",
    "37": "運動休閒",
    "38": "居家生活",
    "91": "管理股票",
}


def _parse_par_value(raw: str) -> float | None:
    """從普通股每股面額字串中擷取數值。

    TWSE/TPEX 的面額格式為 "新台幣 10.0000元"，需擷取其中的數字。

    Args:
        raw: 面額原始字串，例如 "新台幣 10.0000元"。

    Returns:
        面額數值，若無法解析則回傳 None。
    """
    if not raw or not isinstance(raw, str):
        return None
    match = re.search(r"([\d.]+)", raw)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def _safe_int(value: str) -> int:
    """將字串安全轉換為整數。

    處理空字串、含逗號的數字字串等情況。

    Args:
        value: 數字字串。

    Returns:
        轉換後的整數，若無法轉換則回傳 0。
    """
    if not value or not isinstance(value, str):
        return 0
    cleaned = value.replace(",", "").strip()
    if not cleaned:
        return 0
    try:
        return int(cleaned)
    except ValueError:
        try:
            return int(float(cleaned))
        except ValueError:
            return 0


def _calculate_normal_shares(
    paid_in_capital: str,
    par_value_raw: str,
    special_shares: str,
    private_shares: str,
) -> int:
    """計算普通股股數。

    計算方式：實收資本額 / 普通股每股面額 - 特別股股數 - 私募股數

    Args:
        paid_in_capital: 實收資本額字串。
        par_value_raw: 普通股每股面額原始字串。
        special_shares: 特別股股數字串。
        private_shares: 私募股數字串。

    Returns:
        計算後的普通股股數，若面額無法解析則回傳 0。
    """
    par_value = _parse_par_value(par_value_raw)
    if par_value is None or par_value == 0:
        logger.warning(
            "無法解析面額 '%s'，普通股股數設為 0", par_value_raw
        )
        return 0

    capital = _safe_int(paid_in_capital)
    special = _safe_int(special_shares)
    private = _safe_int(private_shares)

    total_shares = int(capital / par_value)
    normal_shares = total_shares - special - private
    return max(normal_shares, 0)


def fetch_twse_company_info() -> list[dict]:
    """從 TWSE OpenAPI 取得上市公司基本資料。

    Returns:
        TWSE API 回傳的公司資料 JSON 列表。

    Raises:
        requests.RequestException: 當 HTTP 請求失敗時。
    """
    logger.info("開始從 TWSE OpenAPI 取得上市公司基本資料")
    response = requests.get(TWSE_API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()
    logger.info("TWSE 上市公司基本資料取得完成，共 %d 筆", len(data))
    return data


def fetch_tpex_company_info() -> list[dict]:
    """從 TPEX OpenAPI 取得上櫃公司基本資料。

    Returns:
        TPEX API 回傳的公司資料 JSON 列表。

    Raises:
        requests.RequestException: 當 HTTP 請求失敗時。
    """
    logger.info("開始從 TPEX OpenAPI 取得上櫃公司基本資料")
    response = requests.get(TPEX_API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()
    logger.info("TPEX 上櫃公司基本資料取得完成，共 %d 筆", len(data))
    return data


def parse_twse_company_info(data: list[dict]) -> pd.DataFrame:
    """解析 TWSE 上市公司基本資料為 DataFrame。

    Args:
        data: TWSE API 回傳的公司資料 JSON 列表。

    Returns:
        包含 SecurityCode, IndustryCode, CompanyName,
        SpecialShares, NormalShares, PrivateShares 的 DataFrame。
    """
    records = []
    for item in data:
        security_code = item.get("公司代號", "").strip()
        company_name = item.get("公司名稱", "").strip()
        industry_code = item.get("產業別", "").strip()
        special_shares_raw = item.get("特別股", "0")
        private_shares_raw = item.get("私募股數", "0")
        paid_in_capital_raw = item.get("實收資本額", "0")
        par_value_raw = item.get("普通股每股面額", "")

        normal_shares = _calculate_normal_shares(
            paid_in_capital_raw,
            par_value_raw,
            special_shares_raw,
            private_shares_raw,
        )

        records.append({
            "SecurityCode": security_code,
            "IndustryCode": industry_code,
            "CompanyName": company_name,
            "SpecialShares": _safe_int(special_shares_raw),
            "NormalShares": normal_shares,
            "PrivateShares": _safe_int(private_shares_raw),
        })

    df = pd.DataFrame(records)
    logger.info("TWSE 上市公司資料解析完成，共 %d 筆", len(df))
    return df


def parse_tpex_company_info(data: list[dict]) -> pd.DataFrame:
    """解析 TPEX 上櫃公司基本資料為 DataFrame。

    Args:
        data: TPEX API 回傳的公司資料 JSON 列表。

    Returns:
        包含 SecurityCode, IndustryCode, CompanyName,
        SpecialShares, NormalShares, PrivateShares 的 DataFrame。
    """
    records = []
    for item in data:
        security_code = item.get("SecuritiesCompanyCode", "").strip()
        company_name = item.get("CompanyName", "").strip()
        industry_code = item.get("SecuritiesIndustryCode", "").strip()
        special_shares_raw = item.get("PreferredStock.shares", "0")
        private_shares_raw = item.get("PrivateStock.shares", "0")
        paid_in_capital_raw = item.get("Paidin.Capital.NTDollars", "0")
        par_value_raw = item.get("ParValueOfCommonStock", "")

        normal_shares = _calculate_normal_shares(
            paid_in_capital_raw,
            par_value_raw,
            special_shares_raw,
            private_shares_raw,
        )

        records.append({
            "SecurityCode": security_code,
            "IndustryCode": industry_code,
            "CompanyName": company_name,
            "SpecialShares": _safe_int(special_shares_raw),
            "NormalShares": normal_shares,
            "PrivateShares": _safe_int(private_shares_raw),
        })

    df = pd.DataFrame(records)
    logger.info("TPEX 上櫃公司資料解析完成，共 %d 筆", len(df))
    return df


def build_industry_map(
    twse_df: pd.DataFrame,
    tpex_df: pd.DataFrame,
) -> list[dict]:
    """從公司資料中建立產業代碼對照表。

    合併 TWSE 與 TPEX 的產業代碼，透過內建對照表映射為產業名稱。
    TWSE 與 TPEX 使用不同的產業代碼體系，分別標註市場別。

    Args:
        twse_df: TWSE 上市公司 DataFrame。
        tpex_df: TPEX 上櫃公司 DataFrame。

    Returns:
        包含 IndustryCode, Industry, Market 的去重列表。
    """
    industry_map = []

    # TWSE 產業對照
    twse_codes = twse_df["IndustryCode"].unique()
    for code in sorted(twse_codes):
        if code:
            industry_map.append({
                "IndustryCode": code,
                "Industry": TWSE_INDUSTRY_MAP.get(code, "未知產業"),
                "Market": "TWSE",
            })

    # TPEX 產業對照
    tpex_codes = tpex_df["IndustryCode"].unique()
    for code in sorted(tpex_codes):
        if code:
            industry_map.append({
                "IndustryCode": code,
                "Industry": TPEX_INDUSTRY_MAP.get(code, "未知產業"),
                "Market": "TPEX",
            })

    logger.info(
        "產業對照表建立完成，TWSE %d 個, TPEX %d 個",
        len(twse_codes),
        len(tpex_codes),
    )
    return industry_map


def company_info_crawler() -> dict:
    """爬取上市與上櫃公司基本資料及產業對照表。

    同時從 TWSE 與 TPEX OpenAPI 取得公司基本資料，
    合併為統一格式並建立產業代碼對照表。

    Returns:
        包含以下鍵值的字典：
        - company_info: 公司基本資料的 dict 列表
        - industry_map: 產業代碼對照表的 dict 列表
        - twse_count: TWSE 上市公司筆數
        - tpex_count: TPEX 上櫃公司筆數
    """
    logger.info("開始爬取公司產業對照資料")

    twse_data = fetch_twse_company_info()
    twse_df = parse_twse_company_info(twse_data)

    tpex_data = fetch_tpex_company_info()
    tpex_df = parse_tpex_company_info(tpex_data)

    # 合併上市與上櫃資料
    combined_df = pd.concat([twse_df, tpex_df], ignore_index=True)

    # 建立產業對照表
    industry_map = build_industry_map(twse_df, tpex_df)

    logger.info(
        "公司產業對照資料爬取完成，共 %d 筆公司資料",
        len(combined_df),
    )

    return {
        "company_info": combined_df.to_dict(orient="records"),
        "industry_map": industry_map,
        "twse_count": len(twse_df),
        "tpex_count": len(tpex_df),
    }
