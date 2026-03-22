"""CTEE 工商時報股市新聞爬蟲測試模組。"""

import json
import pandas as pd
from pytest_mock import MockerFixture

from tw_crawler.ctee_news import (
    _extract_time,
    _gen_empty_df,
    _parse_date_string,
    ctee_news_crawler,
    fetch_article_content,
    fetch_api_page,
    fetch_list_page_html,
    filter_api_articles,
    parse_html_list,
)

# --- 測試用 HTML 模板 ---

SAMPLE_LIST_HTML = """
<html>
<body>
<div class="newslist__card">
    <h3 class="news-title">
        <a href="/news/stock/1001.html">台積電營收創新高</a>
    </h3>
    <time class="news-time">2024-10-15</time>
</div>
<div class="newslist__card">
    <h3 class="news-title">
        <a href="/news/stock/1002.html">聯發科法說會前瞻</a>
    </h3>
    <time class="news-time">2024-10-15</time>
</div>
<div class="newslist__card">
    <h3 class="news-title">
        <a href="/news/stock/1000.html">舊聞標題</a>
    </h3>
    <time class="news-time">2024-10-14</time>
</div>
</body>
</html>
"""

SAMPLE_LIST_HTML_NO_DATE = """
<html>
<body>
<div class="newslist__card">
    <h3 class="news-title">
        <a href="/news/stock/2001.html">無日期的文章</a>
    </h3>
</div>
</body>
</html>
"""

SAMPLE_ARTICLE_HTML = """
<html>
<head>
    <meta name="article:published_time"
          content="2024-10-15T08:30:00+08:00" />
    <meta name="author" content="記者王小明" />
</head>
<body>
<div class="sub-title">第三季營收年增逾三成</div>
<ul>
    <li class="publish-author">記者王小明</li>
    <li class="publish-time">2024-10-15 08:30</li>
</ul>
<li class="taglist__item">台積電</li>
<li class="taglist__item">AI</li>
<li class="taglist__item">半導體</li>
<div class="article-wrap">
    <article>
        <p>台積電今日公布第三季營收數據，受惠於AI晶片需求持續強勁。</p>
        <p>法人預估第四季營收有望再創新高。</p>
        <p>分析師指出，台積電在先進製程的領先優勢將持續帶動獲利成長。</p>
    </article>
</div>
</body>
</html>
"""

SAMPLE_API_RESPONSE = [
    {
        "title": "台積電營收創新高",
        "hyperLink": "/news/stock/1001.html",
        "publishDatetime": "2024-10-15T08:30:00",
        "publishDate": "2024.10.15",
        "author": "記者王小明",
        "content": "台積電今日公布...",
    },
    {
        "title": "聯發科法說會前瞻",
        "hyperLink": "/news/stock/1002.html",
        "publishDatetime": "2024-10-15T10:00:00",
        "publishDate": "2024.10.15",
        "author": "記者李大華",
        "content": "聯發科將舉辦...",
    },
    {
        "title": "舊聞標題",
        "hyperLink": "/news/stock/1000.html",
        "publishDatetime": "2024-10-14T18:00:00",
        "publishDate": "2024.10.14",
        "author": "記者張三",
        "content": "舊聞內容...",
    },
]


# --- 單元測試 ---


def test_parse_date_string_dash() -> None:
    """測試 YYYY-MM-DD 格式日期解析。"""
    from datetime import date
    result = _parse_date_string("發布於 2024-10-15 上午")
    assert result == date(2024, 10, 15)


def test_parse_date_string_slash() -> None:
    """測試 YYYY/MM/DD 格式日期解析。"""
    from datetime import date
    result = _parse_date_string("2024/10/15 08:30")
    assert result == date(2024, 10, 15)


def test_parse_date_string_dot() -> None:
    """測試 YYYY.MM.DD 格式日期解析。"""
    from datetime import date
    result = _parse_date_string("2024.10.15")
    assert result == date(2024, 10, 15)


def test_parse_date_string_chinese() -> None:
    """測試中文日期格式解析。"""
    from datetime import date
    result = _parse_date_string("2024年10月15日")
    assert result == date(2024, 10, 15)


def test_parse_date_string_invalid() -> None:
    """測試無效日期字串回傳 None。"""
    result = _parse_date_string("沒有日期")
    assert result is None


def test_extract_time_iso() -> None:
    """測試 ISO 格式時間提取。"""
    result = _extract_time("2024-10-15T08:30:00+08:00")
    assert result == "08:30:00"


def test_extract_time_hhmm() -> None:
    """測試 HH:MM 格式時間提取。"""
    result = _extract_time("2024/10/15 08:30")
    assert result == "08:30"


def test_extract_time_empty() -> None:
    """測試空字串回傳空字串。"""
    result = _extract_time("")
    assert result == ""


def test_parse_html_list() -> None:
    """測試從列表頁 HTML 解析文章連結。"""
    articles, found_older = parse_html_list(
        SAMPLE_LIST_HTML, "2024-10-15"
    )
    assert len(articles) == 2
    assert articles[0]["url"] == (
        "https://www.ctee.com.tw/news/stock/1001.html"
    )
    assert "台積電" in articles[0]["title"]
    assert articles[1]["url"] == (
        "https://www.ctee.com.tw/news/stock/1002.html"
    )
    assert found_older is True


def test_parse_html_list_no_match() -> None:
    """測試列表頁無符合日期的文章。"""
    articles, found_older = parse_html_list(
        SAMPLE_LIST_HTML, "2024-10-20"
    )
    assert len(articles) == 0
    assert found_older is True


def test_parse_html_list_no_date_in_element() -> None:
    """測試列表頁文章無日期資訊時仍收集連結。"""
    articles, _ = parse_html_list(
        SAMPLE_LIST_HTML_NO_DATE, "2024-10-15"
    )
    assert len(articles) == 1
    assert "2001.html" in articles[0]["url"]


def test_filter_api_articles() -> None:
    """測試 API 文章篩選（日期模式）。"""
    matched, found_older = filter_api_articles(
        SAMPLE_API_RESPONSE, "2024-10-15"
    )
    assert len(matched) == 2
    assert matched[0]["url"] == (
        "https://www.ctee.com.tw/news/stock/1001.html"
    )
    assert found_older is True


def test_filter_api_articles_no_match() -> None:
    """測試 API 篩選無符合文章。"""
    matched, found_older = filter_api_articles(
        SAMPLE_API_RESPONSE, "2024-10-20"
    )
    assert len(matched) == 0
    assert found_older is True


def test_fetch_article_content(mocker: MockerFixture) -> None:
    """測試文章頁面爬取與內容提取。"""
    mock_scraper = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_ARTICLE_HTML
    mock_response.raise_for_status = mocker.Mock()
    mock_scraper.get.return_value = mock_response

    url = "https://www.ctee.com.tw/news/stock/1001.html"
    result = fetch_article_content(mock_scraper, url)

    assert result["SubHead"] == "第三季營收年增逾三成"
    assert result["Author"] == "記者王小明"
    assert "08:30" in result["Time"]
    assert "台積電" in result["HashTag"]
    assert "AI晶片需求" in result["Content"]


def test_gen_empty_df() -> None:
    """測試空 DataFrame 產生。"""
    df = _gen_empty_df()
    assert df.empty
    expected_columns = [
        "Date", "Time", "Author", "Head", "SubHead",
        "HashTag", "url", "Content",
    ]
    assert df.columns.tolist() == expected_columns


def test_fetch_list_page_html(mocker: MockerFixture) -> None:
    """測試首頁列表頁 HTTP 請求。"""
    mock_scraper = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_LIST_HTML
    mock_response.raise_for_status = mocker.Mock()
    mock_scraper.get.return_value = mock_response

    result = fetch_list_page_html(mock_scraper)
    assert result == SAMPLE_LIST_HTML
    mock_scraper.get.assert_called_once_with(
        "https://www.ctee.com.tw/stock/twmarket"
    )


def test_fetch_api_page(mocker: MockerFixture) -> None:
    """測試 JSON API 分頁請求。"""
    mock_scraper = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.text = json.dumps(SAMPLE_API_RESPONSE)
    mock_response.raise_for_status = mocker.Mock()
    mock_scraper.get.return_value = mock_response

    result = fetch_api_page(mock_scraper, 1)
    assert len(result) == 3
    assert result[0]["title"] == "台積電營收創新高"


def test_ctee_news_crawler_success(mocker: MockerFixture) -> None:
    """測試完整爬蟲流程正常回傳。"""
    mocker.patch("tw_crawler.ctee_news.time.sleep")
    mock_scraper = mocker.Mock()
    mocker.patch(
        "tw_crawler.ctee_news._create_scraper",
        return_value=mock_scraper,
    )

    # mock 列表頁回應
    list_response = mocker.Mock()
    list_response.text = SAMPLE_LIST_HTML
    list_response.raise_for_status = mocker.Mock()

    # mock 文章頁回應
    article_response = mocker.Mock()
    article_response.text = SAMPLE_ARTICLE_HTML
    article_response.raise_for_status = mocker.Mock()

    # mock API 回應（第 2 頁，回傳舊文章觸發停止翻頁）
    api_response = mocker.Mock()
    api_response.text = json.dumps([SAMPLE_API_RESPONSE[2]])
    api_response.raise_for_status = mocker.Mock()

    mock_scraper.get.side_effect = [
        list_response,      # 列表頁 HTML
        article_response,   # 文章 1 全文
        article_response,   # 文章 2 全文
        api_response,       # API 第 2 頁（含舊文章，停止翻頁）
    ]

    df = ctee_news_crawler("2024-10-15")

    assert not df.empty
    assert "Date" in df.columns
    assert "Head" in df.columns
    assert "Content" in df.columns
    assert "url" in df.columns
    assert len(df) == 2


def test_ctee_news_crawler_no_articles(mocker: MockerFixture) -> None:
    """測試無符合日期文章時回傳空 DataFrame。"""
    mocker.patch("tw_crawler.ctee_news.time.sleep")
    mock_scraper = mocker.Mock()
    mocker.patch(
        "tw_crawler.ctee_news._create_scraper",
        return_value=mock_scraper,
    )

    list_response = mocker.Mock()
    list_response.text = SAMPLE_LIST_HTML
    list_response.raise_for_status = mocker.Mock()

    # API 回傳也全部是舊文章
    api_response = mocker.Mock()
    api_response.text = json.dumps(SAMPLE_API_RESPONSE)
    api_response.raise_for_status = mocker.Mock()

    mock_scraper.get.side_effect = [
        list_response,    # HTML 首頁（無符合日期）
        api_response,     # API 第 2 頁（也無符合日期）
    ]

    df = ctee_news_crawler("2024-12-25")

    assert df.empty
    expected_columns = [
        "Date", "Time", "Author", "Head", "SubHead",
        "HashTag", "url", "Content",
    ]
    assert df.columns.tolist() == expected_columns


def test_ctee_news_crawler_html_and_api_both_fail(
    mocker: MockerFixture,
) -> None:
    """測試 HTML 首頁和 API 皆失敗時回傳空 DataFrame。"""
    mocker.patch("tw_crawler.ctee_news.time.sleep")
    mock_scraper = mocker.Mock()
    mocker.patch(
        "tw_crawler.ctee_news._create_scraper",
        return_value=mock_scraper,
    )

    mock_scraper.get.side_effect = Exception("Connection error")

    df = ctee_news_crawler("2024-10-15")

    assert df.empty


def test_ctee_news_crawler_html_fail_api_fallback(
    mocker: MockerFixture,
) -> None:
    """測試 HTML 首頁失敗時，API 從第 1 頁開始補抓（fallback 機制）。"""
    mocker.patch("tw_crawler.ctee_news.time.sleep")
    mock_scraper = mocker.Mock()
    mocker.patch(
        "tw_crawler.ctee_news._create_scraper",
        return_value=mock_scraper,
    )

    # 第 1 次呼叫：HTML 首頁失敗
    html_error = Exception("403 Forbidden")

    # API 第 1 頁回應（含 2 篇符合日期的文章 + 1 篇舊文章）
    api_page1_response = mocker.Mock()
    api_page1_response.text = json.dumps(SAMPLE_API_RESPONSE)
    api_page1_response.raise_for_status = mocker.Mock()

    # 文章全文頁回應
    article_response = mocker.Mock()
    article_response.text = SAMPLE_ARTICLE_HTML
    article_response.raise_for_status = mocker.Mock()

    mock_scraper.get.side_effect = [
        html_error,              # HTML 首頁失敗
        api_page1_response,      # API 第 1 頁（fallback）
        article_response,        # 文章 1 全文
        article_response,        # 文章 2 全文
    ]

    df = ctee_news_crawler("2024-10-15")

    assert not df.empty
    assert len(df) == 2
    assert "Date" in df.columns
    assert "Content" in df.columns

    # 驗證 API 確實從第 1 頁開始呼叫
    # 第 1 次 get 是 HTML 首頁（失敗），第 2 次是 API 第 1 頁
    api_call = mock_scraper.get.call_args_list[1]
    assert "api/category/twmarket/1" in api_call[0][0]
