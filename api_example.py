"""本地端 API 請求範例。

示範如何對本地 FastAPI server 發送請求並顯示回傳結果。
"""

import json
import os

import requests

BASE_URL = os.environ.get("API_BASE_URL", "http://127.0.0.1:6738")


def show_response(name: str, url: str) -> None:
    """發送 GET 請求並印出回傳結果。

    Args:
        name: 請求名稱。
        url: 請求 URL。
    """
    print(f"{'=' * 60}")
    print(f"請求: {name}")
    print(f"URL:  {url}")
    print(f"{'=' * 60}")
    try:
        resp = requests.get(url, timeout=30)
        data = resp.json()
        print(json.dumps(data, indent=2, ensure_ascii=False))
    except requests.exceptions.ConnectionError:
        print("連線失敗，請確認 Docker container 是否已啟動")
    print()


if __name__ == "__main__":
    # 爬取當天所有資料
    show_response("所有資料（當天）", f"{BASE_URL}/")

    # 爬取指定日期所有資料
    show_response("所有資料（指定日期）", f"{BASE_URL}/?date=2024-10-29")

    # 各別爬蟲
    show_response("上市股票（TWSE）", f"{BASE_URL}/twse?date=2024-10-29")
    show_response("上櫃股票（TPEX）", f"{BASE_URL}/tpex?date=2024-10-29")
    show_response("期貨（TAIFEX）", f"{BASE_URL}/taifex?date=2024-10-29")
    show_response("三大法人（FAOI）", f"{BASE_URL}/faoi?date=2024-10-29")
    show_response("融資融券（MGTS）", f"{BASE_URL}/mgts?date=2024-10-29")
    show_response("集保戶股權分散表（TDCC）", f"{BASE_URL}/tdcc")
