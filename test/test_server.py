"""FastAPI server endpoint 測試模組。"""

import datetime

import pandas as pd
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

from server import app

client = TestClient(app)


def _mock_crawler(date: str) -> pd.DataFrame:
    """建立假的爬蟲回傳 DataFrame。"""
    return pd.DataFrame({
        "Date": [date],
        "SecurityCode": ["2330"],
        "ClosingPrice": [600.0],
    })


def test_crawl_all_success(mocker: MockerFixture) -> None:
    """測試 GET / 正常回傳所有爬蟲資料。"""
    mocker.patch("server.CRAWLERS", {
        "twse": _mock_crawler,
        "tpex": _mock_crawler,
    })
    today = datetime.date.today().strftime("%Y-%m-%d")

    response = client.get("/")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == today
    assert "twse" in data["data"]
    assert "tpex" in data["data"]
    assert len(data["data"]["twse"]) == 1
    assert data["data"]["twse"][0]["SecurityCode"] == "2330"


def test_crawl_all_with_date(mocker: MockerFixture) -> None:
    """測試 GET /?date=YYYY-MM-DD 回傳指定日期的資料。"""
    mocker.patch("server.CRAWLERS", {
        "twse": _mock_crawler,
    })
    query_date = "2024-10-29"

    response = client.get(f"/?date={query_date}")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == query_date
    assert data["data"]["twse"][0]["Date"] == query_date


def test_crawl_all_partial_failure(mocker: MockerFixture) -> None:
    """測試部分爬蟲失敗時，仍回傳其他成功的資料。"""
    def _failing_crawler(date: str) -> pd.DataFrame:
        raise RuntimeError("connection error")

    mocker.patch("server.CRAWLERS", {
        "twse": _mock_crawler,
        "tpex": _failing_crawler,
    })

    response = client.get("/")

    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]["twse"]) == 1
    assert "error" in data["data"]["tpex"]


def test_crawl_twse(mocker: MockerFixture) -> None:
    """測試 GET /twse 回傳上市股票資料。"""
    mocker.patch("server.CRAWLERS", {"twse": _mock_crawler})

    response = client.get("/twse?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == "2024-10-29"
    assert data["data"][0]["SecurityCode"] == "2330"


def test_crawl_tpex(mocker: MockerFixture) -> None:
    """測試 GET /tpex 回傳上櫃股票資料。"""
    mocker.patch("server.CRAWLERS", {"tpex": _mock_crawler})

    response = client.get("/tpex?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == "2024-10-29"
    assert data["data"][0]["SecurityCode"] == "2330"


def test_crawl_taifex(mocker: MockerFixture) -> None:
    """測試 GET /taifex 回傳期貨資料。"""
    mocker.patch("server.CRAWLERS", {"taifex": _mock_crawler})

    response = client.get("/taifex?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == "2024-10-29"


def test_crawl_faoi(mocker: MockerFixture) -> None:
    """測試 GET /faoi 回傳三大法人資料。"""
    mocker.patch("server.CRAWLERS", {"faoi": _mock_crawler})

    response = client.get("/faoi?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == "2024-10-29"


def test_crawl_mgts(mocker: MockerFixture) -> None:
    """測試 GET /mgts 回傳融資融券資料。"""
    mocker.patch("server.CRAWLERS", {"mgts": _mock_crawler})

    response = client.get("/mgts?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == "2024-10-29"


def test_crawl_tdcc(mocker: MockerFixture) -> None:
    """測試 GET /tdcc 回傳集保戶股權分散表資料。"""
    mocker.patch("server.CRAWLERS", {"tdcc": _mock_crawler})

    response = client.get("/tdcc?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == "2024-10-29"
    assert data["data"][0]["SecurityCode"] == "2330"


def test_crawl_single_failure(mocker: MockerFixture) -> None:
    """測試單一爬蟲失敗時回傳 error。"""
    def _failing_crawler(date: str) -> pd.DataFrame:
        raise RuntimeError("connection error")

    mocker.patch("server.CRAWLERS", {"twse": _failing_crawler})

    response = client.get("/twse?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert "error" in data
    assert data["date"] == "2024-10-29"
