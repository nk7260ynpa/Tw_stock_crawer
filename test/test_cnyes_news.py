"""鉅亨網台股新聞爬蟲測試模組。"""

import pandas as pd
from pytest_mock import MockerFixture

from tw_crawler.cnyes_news import (
    _build_article_url,
    _gen_empty_df,
    _html_to_markdown,
    _keywords_to_hashtag,
    _timestamp_to_tw_datetime,
    cnyes_news_crawler,
    fetch_news_page,
    parse_news_items,
)

# --- 測試用 API 回應資料 ---

# 2024-10-15 08:30:00 UTC+8 = 2024-10-15 00:30:00 UTC = 1728952200
# 2024-10-15 10:15:00 UTC+8 = 2024-10-15 02:15:00 UTC = 1728958500
# 2024-10-14 18:00:00 UTC+8 = 2024-10-14 10:00:00 UTC = 1728900000

SAMPLE_API_RESPONSE = {
    "statusCode": 200,
    "message": "OK",
    "items": {
        "data": [
            {
                "newsId": 5001,
                "title": "台積電法說會釋利多 外資連買三日",
                "content": "<p>台積電今日召開法說會，釋出正面展望。</p>"
                           "<p>外資連續三個交易日買超。</p>",
                "publishAt": 1728952200,  # 2024-10-15 08:30:00 UTC+8
                "keyword": ["台積電", "法說會", "外資"],
                "categoryName": "台股新聞",
                "author": "記者李小華",
            },
            {
                "newsId": 5002,
                "title": "聯發科新晶片量產在即",
                "content": "<p>聯發科天璣新一代晶片即將量產。</p>",
                "publishAt": 1728958500,  # 2024-10-15 10:15:00 UTC+8
                "keyword": ["聯發科", "晶片"],
                "categoryName": "台股新聞",
                "author": "",
            },
            {
                "newsId": 5000,
                "title": "昨日舊聞",
                "content": "<p>這是昨天的新聞。</p>",
                "publishAt": 1728900000,  # 2024-10-14 18:00:00 UTC+8
                "keyword": ["大盤"],
                "categoryName": "台股新聞",
                "author": "記者張三",
            },
        ],
        "last_page": 3,
    },
}

SAMPLE_API_RESPONSE_EMPTY = {
    "statusCode": 200,
    "message": "OK",
    "items": {
        "data": [],
        "last_page": 1,
    },
}

SAMPLE_API_RESPONSE_PAGE2 = {
    "statusCode": 200,
    "message": "OK",
    "items": {
        "data": [
            {
                "newsId": 5003,
                "title": "第二頁新聞",
                "content": "<p>第二頁內容。</p>",
                "publishAt": 1728921600,  # 2024-10-15 00:00:00 UTC+8
                "keyword": ["台股"],
                "categoryName": "台股新聞",
                "author": "",
            },
        ],
        "last_page": 3,
    },
}


# --- 單元測試：工具函式 ---


def test_timestamp_to_tw_datetime() -> None:
    """測試 Unix timestamp 轉台灣時區 datetime。"""
    # 2024-10-15 08:30:00 UTC+8
    dt = _timestamp_to_tw_datetime(1728952200)
    assert dt.year == 2024
    assert dt.month == 10
    assert dt.day == 15
    assert dt.hour == 8
    assert dt.minute == 30


def test_html_to_markdown_normal() -> None:
    """測試 HTML 轉 Markdown（一般段落）。"""
    html = "<p>第一段文字。</p><p>第二段文字。</p>"
    result = _html_to_markdown(html)
    assert "第一段文字。" in result
    assert "第二段文字。" in result


def test_html_to_markdown_empty() -> None:
    """測試 HTML 轉 Markdown（空字串）。"""
    assert _html_to_markdown("") == ""
    assert _html_to_markdown(None) == ""


def test_html_to_markdown_with_tags() -> None:
    """測試 HTML 轉 Markdown（含連結與粗體）。"""
    html = "<p>這是<strong>重要</strong>的<a href='#'>連結</a>。</p>"
    result = _html_to_markdown(html)
    assert "重要" in result
    assert "連結" in result


def test_keywords_to_hashtag_normal() -> None:
    """測試 keyword 陣列轉 HashTag 字串。"""
    result = _keywords_to_hashtag(["台積電", "AI", "半導體"])
    assert result == "台積電,AI,半導體"


def test_keywords_to_hashtag_empty() -> None:
    """測試空 keyword 陣列。"""
    assert _keywords_to_hashtag([]) == ""
    assert _keywords_to_hashtag(None) == ""


def test_keywords_to_hashtag_with_empty_items() -> None:
    """測試 keyword 陣列中包含空字串。"""
    result = _keywords_to_hashtag(["台積電", "", "AI"])
    assert result == "台積電,AI"


def test_build_article_url() -> None:
    """測試文章 URL 組成。"""
    url = _build_article_url(12345)
    assert url == "https://news.cnyes.com/news/id/12345"


def test_gen_empty_df() -> None:
    """測試空 DataFrame 產生。"""
    df = _gen_empty_df()
    assert df.empty
    expected_columns = [
        "Date", "Time", "Author", "Head", "HashTag", "url", "Content",
    ]
    assert df.columns.tolist() == expected_columns


# --- 單元測試：parse_news_items ---


def test_parse_news_items_filter_by_date() -> None:
    """測試日期篩選：只保留目標日期的文章。"""
    items = SAMPLE_API_RESPONSE["items"]["data"]
    matched, found_older = parse_news_items(items, "2024-10-15")

    # 應匹配 2 篇（newsId 5001, 5002），排除 1 篇舊聞（newsId 5000）
    assert len(matched) == 2
    assert found_older is True

    # 檢查第一篇
    assert matched[0]["Head"] == "台積電法說會釋利多 外資連買三日"
    assert matched[0]["Date"] == "2024-10-15"
    assert matched[0]["Time"] == "08:30:00"
    assert matched[0]["Author"] == "記者李小華"
    assert matched[0]["url"] == "https://news.cnyes.com/news/id/5001"
    assert "台積電,法說會,外資" == matched[0]["HashTag"]

    # 檢查第二篇（Author 為空字串）
    assert matched[1]["Head"] == "聯發科新晶片量產在即"
    assert matched[1]["Author"] == ""
    assert matched[1]["Time"] == "10:15:00"


def test_parse_news_items_no_match() -> None:
    """測試無符合日期的文章。"""
    items = SAMPLE_API_RESPONSE["items"]["data"]
    matched, found_older = parse_news_items(items, "2024-10-20")

    assert len(matched) == 0
    assert found_older is True  # 所有文章都比 10-20 早


def test_parse_news_items_all_match() -> None:
    """測試所有文章都在目標日期（無更早文章）。"""
    items = [
        {
            "newsId": 6001,
            "title": "新聞A",
            "content": "<p>內容A</p>",
            "publishAt": 1728923400,  # 2024-10-15 08:30:00 UTC+8
            "keyword": ["A"],
            "author": "作者A",
        },
    ]
    matched, found_older = parse_news_items(items, "2024-10-15")

    assert len(matched) == 1
    assert found_older is False


def test_parse_news_items_content_markdown_conversion() -> None:
    """測試 HTML content 轉為 Markdown。"""
    items = [
        {
            "newsId": 7001,
            "title": "測試",
            "content": "<p>第一段。</p><p><strong>粗體</strong>文字。</p>",
            "publishAt": 1728923400,
            "keyword": [],
            "author": "",
        },
    ]
    matched, _ = parse_news_items(items, "2024-10-15")

    assert len(matched) == 1
    assert "第一段。" in matched[0]["Content"]
    assert "粗體" in matched[0]["Content"]


def test_parse_news_items_missing_publish_at() -> None:
    """測試缺少 publishAt 欄位的項目被跳過。"""
    items = [
        {
            "newsId": 8001,
            "title": "缺少時間的新聞",
            "content": "<p>內容</p>",
            "keyword": [],
            "author": "",
        },
    ]
    matched, found_older = parse_news_items(items, "2024-10-15")

    assert len(matched) == 0
    assert found_older is False


# --- 單元測試：fetch_news_page ---


def test_fetch_news_page_success(mocker: MockerFixture) -> None:
    """測試 API 請求成功。"""
    mock_response = mocker.Mock()
    mock_response.json.return_value = SAMPLE_API_RESPONSE
    mock_response.raise_for_status = mocker.Mock()
    mocker.patch("tw_crawler.cnyes_news.requests.get", return_value=mock_response)

    result = fetch_news_page(1)

    assert result["statusCode"] == 200
    assert len(result["items"]["data"]) == 3


def test_fetch_news_page_error_status(mocker: MockerFixture) -> None:
    """測試 API 回傳非 200 狀態碼。"""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "statusCode": 500,
        "message": "Internal Server Error",
    }
    mock_response.raise_for_status = mocker.Mock()
    mocker.patch("tw_crawler.cnyes_news.requests.get", return_value=mock_response)

    import pytest
    with pytest.raises(ValueError, match="非 200 狀態碼"):
        fetch_news_page(1)


# --- 單元測試：cnyes_news_crawler ---


def test_cnyes_news_crawler_success(mocker: MockerFixture) -> None:
    """測試完整爬蟲流程正常回傳。"""
    mock_response = mocker.Mock()
    mock_response.json.return_value = SAMPLE_API_RESPONSE
    mock_response.raise_for_status = mocker.Mock()
    mocker.patch("tw_crawler.cnyes_news.requests.get", return_value=mock_response)

    df = cnyes_news_crawler("2024-10-15")

    assert not df.empty
    assert len(df) == 2
    assert list(df.columns) == [
        "Date", "Time", "Author", "Head", "HashTag", "url", "Content",
    ]
    assert df.iloc[0]["Head"] == "台積電法說會釋利多 外資連買三日"
    assert df.iloc[0]["Date"] == "2024-10-15"
    assert df.iloc[0]["Time"] == "08:30:00"
    assert "台積電" in df.iloc[0]["HashTag"]
    assert "法說會" in df.iloc[0]["Content"]


def test_cnyes_news_crawler_no_articles(mocker: MockerFixture) -> None:
    """測試無符合日期文章時回傳空 DataFrame。"""
    mock_response = mocker.Mock()
    mock_response.json.return_value = SAMPLE_API_RESPONSE
    mock_response.raise_for_status = mocker.Mock()
    mocker.patch("tw_crawler.cnyes_news.requests.get", return_value=mock_response)

    df = cnyes_news_crawler("2024-10-20")

    assert df.empty
    expected_columns = [
        "Date", "Time", "Author", "Head", "HashTag", "url", "Content",
    ]
    assert df.columns.tolist() == expected_columns


def test_cnyes_news_crawler_empty_api(mocker: MockerFixture) -> None:
    """測試 API 回傳空資料。"""
    mock_response = mocker.Mock()
    mock_response.json.return_value = SAMPLE_API_RESPONSE_EMPTY
    mock_response.raise_for_status = mocker.Mock()
    mocker.patch("tw_crawler.cnyes_news.requests.get", return_value=mock_response)

    df = cnyes_news_crawler("2024-10-15")

    assert df.empty


def test_cnyes_news_crawler_multi_page(mocker: MockerFixture) -> None:
    """測試多頁翻頁爬取。"""
    # 第 1 頁：只有符合日期的文章（無更早文章，需繼續翻頁）
    page1_data = {
        "statusCode": 200,
        "message": "OK",
        "items": {
            "data": [
                {
                    "newsId": 5001,
                    "title": "第一頁新聞",
                    "content": "<p>第一頁內容。</p>",
                    "publishAt": 1728952200,  # 2024-10-15 08:30 UTC+8
                    "keyword": ["台股"],
                    "author": "",
                },
            ],
            "last_page": 2,
        },
    }
    # 第 2 頁：有更早的文章，應停止翻頁
    page2_data = SAMPLE_API_RESPONSE  # 包含 10-14 的文章

    mock_response_p1 = mocker.Mock()
    mock_response_p1.json.return_value = page1_data
    mock_response_p1.raise_for_status = mocker.Mock()

    mock_response_p2 = mocker.Mock()
    mock_response_p2.json.return_value = page2_data
    mock_response_p2.raise_for_status = mocker.Mock()

    mocker.patch(
        "tw_crawler.cnyes_news.requests.get",
        side_effect=[mock_response_p1, mock_response_p2],
    )

    df = cnyes_news_crawler("2024-10-15")

    assert not df.empty
    # 第 1 頁 1 篇 + 第 2 頁 2 篇 = 3 篇
    assert len(df) == 3


def test_cnyes_news_crawler_api_failure(mocker: MockerFixture) -> None:
    """測試 API 請求失敗時回傳空 DataFrame。"""
    import requests as req
    mocker.patch(
        "tw_crawler.cnyes_news.requests.get",
        side_effect=req.RequestException("Connection error"),
    )

    df = cnyes_news_crawler("2024-10-15")

    assert df.empty


# --- 單元測試：時數模式（hours） ---


def test_parse_news_items_hours_mode() -> None:
    """測試時數模式篩選：只保留 cutoff_dt 之後的文章。"""
    from datetime import datetime, timezone, timedelta
    TW_TZ = timezone(timedelta(hours=8))

    items = SAMPLE_API_RESPONSE["items"]["data"]
    # cutoff 設在 2024-10-15 09:00 UTC+8，應只匹配 10:15 的那篇
    cutoff = datetime(2024, 10, 15, 9, 0, 0, tzinfo=TW_TZ)
    matched, found_older = parse_news_items(
        items, "2024-10-15", cutoff_dt=cutoff,
    )

    assert len(matched) == 1
    assert matched[0]["Head"] == "聯發科新晶片量產在即"
    assert matched[0]["Date"] == "2024-10-15"
    assert matched[0]["Time"] == "10:15:00"
    assert found_older is True


def test_parse_news_items_hours_mode_all_match() -> None:
    """測試時數模式：所有文章都在 cutoff 之後。"""
    from datetime import datetime, timezone, timedelta
    TW_TZ = timezone(timedelta(hours=8))

    items = SAMPLE_API_RESPONSE["items"]["data"]
    # cutoff 設在 2024-10-14 00:00 UTC+8，所有文章都在之後
    cutoff = datetime(2024, 10, 14, 0, 0, 0, tzinfo=TW_TZ)
    matched, found_older = parse_news_items(
        items, "2024-10-15", cutoff_dt=cutoff,
    )

    assert len(matched) == 3
    assert found_older is False


def test_parse_news_items_hours_mode_cross_day() -> None:
    """測試時數模式跨日：cutoff 在前一天，應包含前一天晚上的文章。"""
    from datetime import datetime, timezone, timedelta
    TW_TZ = timezone(timedelta(hours=8))

    items = SAMPLE_API_RESPONSE["items"]["data"]
    # cutoff 設在 2024-10-14 17:00 UTC+8
    # newsId 5000 的 publishAt 是 2024-10-14 18:00 UTC+8，應被包含
    cutoff = datetime(2024, 10, 14, 17, 0, 0, tzinfo=TW_TZ)
    matched, found_older = parse_news_items(
        items, "2024-10-15", cutoff_dt=cutoff,
    )

    assert len(matched) == 3
    # 檢查跨日文章的 Date 欄位應是文章自己的日期
    dates = {m["Date"] for m in matched}
    assert "2024-10-14" in dates
    assert "2024-10-15" in dates


def test_cnyes_news_crawler_hours_mode(mocker: MockerFixture) -> None:
    """測試完整爬蟲流程（時數模式）。"""
    from datetime import datetime, timezone, timedelta
    TW_TZ = timezone(timedelta(hours=8))

    # Mock datetime.now 回傳 2024-10-15 12:00 UTC+8
    mock_now = datetime(2024, 10, 15, 12, 0, 0, tzinfo=TW_TZ)
    mocker.patch(
        "tw_crawler.cnyes_news.datetime",
        wraps=datetime,
    )
    mocker.patch(
        "tw_crawler.cnyes_news.datetime.now",
        return_value=mock_now,
    )

    # 使用包含更早文章的 API 回應，這樣 found_older=True 會停止翻頁
    # hours=4 -> cutoff = 2024-10-15 08:00 UTC+8
    # newsId 5001 (08:30) 和 5002 (10:15) 在 cutoff 之後
    # newsId 5000 (10-14 18:00) 在 cutoff 之前 -> 停止翻頁
    mock_response = mocker.Mock()
    mock_response.json.return_value = SAMPLE_API_RESPONSE
    mock_response.raise_for_status = mocker.Mock()
    mocker.patch(
        "tw_crawler.cnyes_news.requests.get",
        return_value=mock_response,
    )

    df = cnyes_news_crawler("2024-10-15", hours=4)

    assert not df.empty
    assert len(df) == 2  # 只有 08:30 和 10:15 在 cutoff 之後
    assert list(df.columns) == [
        "Date", "Time", "Author", "Head", "HashTag", "url", "Content",
    ]
    assert df.iloc[0]["Date"] == "2024-10-15"
    assert df.iloc[0]["Time"] == "08:30:00"
