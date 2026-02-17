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
