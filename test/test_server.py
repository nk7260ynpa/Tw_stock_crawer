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


def test_crawl_ctee_news(mocker: MockerFixture) -> None:
    """測試 GET /ctee_news 回傳 CTEE 新聞資料。"""
    mock_df = pd.DataFrame({
        "Date": ["2024-10-29"],
        "Time": ["2024-10-29T08:30:00+08:00"],
        "Author": ["記者王小明"],
        "Head": ["台積電營收創新高"],
        "SubHead": ["AI需求帶動"],
        "HashTag": ["台積電,AI"],
        "url": ["https://www.ctee.com.tw/news/stock/1001.html"],
        "Content": ["台積電第三季營收數據亮眼。"],
    })
    mocker.patch(
        "server.tw_crawler.ctee_news_crawler",
        return_value=mock_df,
    )

    response = client.get("/ctee_news?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == "2024-10-29"
    assert len(data["data"]) == 1
    assert data["data"][0]["Head"] == "台積電營收創新高"


def test_crawl_ctee_news_failure(mocker: MockerFixture) -> None:
    """測試 CTEE 新聞爬蟲失敗時回傳 error。"""
    mocker.patch(
        "server.tw_crawler.ctee_news_crawler",
        side_effect=RuntimeError("connection error"),
    )

    response = client.get("/ctee_news?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert "error" in data
    assert data["date"] == "2024-10-29"


def test_crawl_cnyes_news(mocker: MockerFixture) -> None:
    """測試 GET /cnyes_news 回傳鉅亨網新聞資料。"""
    mock_df = pd.DataFrame({
        "Date": ["2024-10-29"],
        "Time": ["08:30:00"],
        "Author": ["記者李小華"],
        "Head": ["台積電法說會釋利多"],
        "HashTag": ["台積電,法說會"],
        "url": ["https://news.cnyes.com/news/id/5001"],
        "Content": ["台積電今日召開法說會。"],
    })
    mocker.patch(
        "server.tw_crawler.cnyes_news_crawler",
        return_value=mock_df,
    )

    response = client.get("/cnyes_news?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == "2024-10-29"
    assert len(data["data"]) == 1
    assert data["data"][0]["Head"] == "台積電法說會釋利多"


def test_crawl_cnyes_news_failure(mocker: MockerFixture) -> None:
    """測試鉅亨網新聞爬蟲失敗時回傳 error。"""
    mocker.patch(
        "server.tw_crawler.cnyes_news_crawler",
        side_effect=RuntimeError("connection error"),
    )

    response = client.get("/cnyes_news?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert "error" in data
    assert data["date"] == "2024-10-29"


def test_crawl_ptt_news(mocker: MockerFixture) -> None:
    """測試 GET /ptt_news 回傳 PTT 股版文章資料。"""
    mock_df = pd.DataFrame({
        "Date": ["2024-10-29"],
        "Time": ["14:30:00"],
        "Author": ["stockman"],
        "Head": ["[新聞] 台積電法說會釋利多"],
        "url": ["https://www.ptt.cc/bbs/stock/M.123.A.B01.html"],
        "Content": ["台積電今日召開法說會。"],
    })
    mocker.patch(
        "server.tw_crawler.ptt_news_crawler",
        return_value=mock_df,
    )

    response = client.get("/ptt_news?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == "2024-10-29"
    assert len(data["data"]) == 1
    assert data["data"][0]["Head"] == "[新聞] 台積電法說會釋利多"


def test_crawl_ptt_news_failure(mocker: MockerFixture) -> None:
    """測試 PTT 股版爬蟲失敗時回傳 error。"""
    mocker.patch(
        "server.tw_crawler.ptt_news_crawler",
        side_effect=RuntimeError("connection error"),
    )

    response = client.get("/ptt_news?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert "error" in data
    assert data["date"] == "2024-10-29"


def test_crawl_moneyudn_news(mocker: MockerFixture) -> None:
    """測試 GET /moneyudn_news 回傳聯合新聞網經濟日報新聞資料。"""
    mock_df = pd.DataFrame({
        "Date": ["2024-10-29"],
        "Time": ["08:30:00"],
        "Author": ["記者鐘惠玲"],
        "Head": ["台積電法說會釋利多 外資連買三日"],
        "url": ["https://money.udn.com/money/story/5612/9348922"],
        "Content": ["台積電今日召開法說會。"],
    })
    mocker.patch(
        "server.tw_crawler.moneyudn_news_crawler",
        return_value=mock_df,
    )

    response = client.get("/moneyudn_news?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert data["date"] == "2024-10-29"
    assert len(data["data"]) == 1
    assert data["data"][0]["Head"] == "台積電法說會釋利多 外資連買三日"


def test_crawl_moneyudn_news_failure(mocker: MockerFixture) -> None:
    """測試 MoneyUDN 新聞爬蟲失敗時回傳 error。"""
    mocker.patch(
        "server.tw_crawler.moneyudn_news_crawler",
        side_effect=RuntimeError("connection error"),
    )

    response = client.get("/moneyudn_news?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert "error" in data
    assert data["date"] == "2024-10-29"


# --- 時數模式（hours 參數）測試 ---


def _mock_news_df(date: str, hours: int | None = None) -> pd.DataFrame:
    """建立假的新聞爬蟲回傳 DataFrame。"""
    return pd.DataFrame({
        "Date": [date],
        "Time": ["14:30:00"],
        "Author": ["記者"],
        "Head": ["測試新聞"],
        "url": ["https://example.com/news/1"],
        "Content": ["測試內容"],
    })


def test_crawl_ctee_news_with_hours(mocker: MockerFixture) -> None:
    """測試 GET /ctee_news?hours=24 回傳含 hours 欄位。"""
    mocker.patch(
        "server.tw_crawler.ctee_news_crawler",
        side_effect=_mock_news_df,
    )

    response = client.get("/ctee_news?hours=24")

    assert response.status_code == 200
    data = response.json()
    assert data["hours"] == 24
    assert len(data["data"]) == 1


def test_crawl_cnyes_news_with_hours(mocker: MockerFixture) -> None:
    """測試 GET /cnyes_news?hours=24 回傳含 hours 欄位。"""
    mocker.patch(
        "server.tw_crawler.cnyes_news_crawler",
        side_effect=_mock_news_df,
    )

    response = client.get("/cnyes_news?hours=24")

    assert response.status_code == 200
    data = response.json()
    assert data["hours"] == 24
    assert len(data["data"]) == 1


def test_crawl_ptt_news_with_hours(mocker: MockerFixture) -> None:
    """測試 GET /ptt_news?hours=24 回傳含 hours 欄位。"""
    mocker.patch(
        "server.tw_crawler.ptt_news_crawler",
        side_effect=_mock_news_df,
    )

    response = client.get("/ptt_news?hours=24")

    assert response.status_code == 200
    data = response.json()
    assert data["hours"] == 24
    assert len(data["data"]) == 1


def test_crawl_moneyudn_news_with_hours(mocker: MockerFixture) -> None:
    """測試 GET /moneyudn_news?hours=24 回傳含 hours 欄位。"""
    mocker.patch(
        "server.tw_crawler.moneyudn_news_crawler",
        side_effect=_mock_news_df,
    )

    response = client.get("/moneyudn_news?hours=24")

    assert response.status_code == 200
    data = response.json()
    assert data["hours"] == 24
    assert len(data["data"]) == 1


def test_crawl_news_hours_validation() -> None:
    """測試 hours 參數驗證（範圍 1-72）。"""
    response = client.get("/cnyes_news?hours=0")
    assert response.status_code == 422  # FastAPI validation error

    response = client.get("/cnyes_news?hours=73")
    assert response.status_code == 422


def test_crawl_news_without_hours_no_hours_field(
    mocker: MockerFixture,
) -> None:
    """測試不帶 hours 參數時回傳不含 hours 欄位。"""
    mock_df = pd.DataFrame({
        "Date": ["2024-10-29"],
        "Time": ["08:30:00"],
        "Author": ["記者"],
        "Head": ["測試新聞"],
        "HashTag": ["測試"],
        "url": ["https://example.com/news/1"],
        "Content": ["測試內容"],
    })
    mocker.patch(
        "server.tw_crawler.cnyes_news_crawler",
        return_value=mock_df,
    )

    response = client.get("/cnyes_news?date=2024-10-29")

    assert response.status_code == 200
    data = response.json()
    assert "hours" not in data
    assert data["date"] == "2024-10-29"
