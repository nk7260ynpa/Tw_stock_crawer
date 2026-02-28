"""聯合新聞網經濟日報台股新聞爬蟲測試模組。"""

import pandas as pd
import pytest
import requests
from pytest_mock import MockerFixture

from tw_crawler.moneyudn_news import (
    DEFAULT_HEADERS,
    MAX_LIST_PAGES,
    _build_full_url,
    _collect_candidates_from_pages,
    _create_session,
    _extract_author,
    _extract_hero_image,
    _fetch_article_content,
    _gen_empty_df,
    _parse_list_page,
    _parse_published_date,
    moneyudn_news_crawler,
)

# --- 測試用 HTML / JSON-LD 資料 ---

SAMPLE_LIST_HTML = """
<html>
<head>
<script type="application/ld+json">
{
    "@context": "https://schema.org",
    "@graph": [
        {
            "@type": "CollectionPage",
            "@id": "https://money.udn.com/rank/pv/1001/5591/1",
            "name": "熱門排行榜 - 經濟日報"
        },
        {
            "@type": "ItemList",
            "itemListOrder": "ItemListOrderDescending",
            "numberOfItems": 3,
            "itemListElement": [
                {
                    "@type": "ListItem",
                    "position": 1,
                    "item": {
                        "@type": "NewsArticle",
                        "url": "/money/story/5612/9348922",
                        "name": "台積電法說會釋利多 外資連買三日",
                        "headline": "台積電法說會釋利多 外資連買三日",
                        "datePublished": "2026-02-27T08:30:00+08:00",
                        "author": {
                            "@type": "Person",
                            "name": "記者鐘惠玲"
                        },
                        "image": {
                            "@type": "ImageObject",
                            "url": "https://pgw.udn.com.tw/gw/photo.php?u=test.jpg"
                        },
                        "isAccessibleForFree": true
                    }
                },
                {
                    "@type": "ListItem",
                    "position": 2,
                    "item": {
                        "@type": "NewsArticle",
                        "url": "/money/story/5612/9348923",
                        "name": "聯發科新晶片量產在即",
                        "headline": "聯發科新晶片量產在即",
                        "datePublished": "2026-02-27T10:15:00+08:00",
                        "author": {
                            "@type": "Person",
                            "name": "記者李小華"
                        },
                        "isAccessibleForFree": true
                    }
                },
                {
                    "@type": "ListItem",
                    "position": 3,
                    "item": {
                        "@type": "NewsArticle",
                        "url": "/money/story/5612/9348900",
                        "name": "昨日舊聞標題",
                        "headline": "昨日舊聞標題",
                        "datePublished": "2026-02-26T18:00:00+08:00",
                        "author": {
                            "@type": "Person",
                            "name": "記者張三"
                        },
                        "isAccessibleForFree": true
                    }
                }
            ]
        }
    ]
}
</script>
</head>
<body></body>
</html>
"""

SAMPLE_LIST_HTML_NO_JSONLD = """
<html>
<head></head>
<body><p>沒有 JSON-LD</p></body>
</html>
"""

SAMPLE_LIST_HTML_EMPTY_ITEMLIST = """
<html>
<head>
<script type="application/ld+json">
{
    "@context": "https://schema.org",
    "@graph": [
        {
            "@type": "ItemList",
            "itemListOrder": "ItemListOrderDescending",
            "numberOfItems": 0,
            "itemListElement": []
        }
    ]
}
</script>
</head>
<body></body>
</html>
"""

SAMPLE_ARTICLE_HTML = """
<html>
<body>
<div id="article_body" class="article-body">
    <p>台積電今日召開法說會，釋出正面展望。</p>
    <p>外資連續三個交易日買超。</p>
    <figure>
        <img src="https://pgw.udn.com.tw/gw/photo.php?u=https://uc.udn.com.tw/photo/2026/02/27/test.jpg&x=0&y=0&sw=0&sh=0&exp=3600&w=441"
             alt="台積電董事長（左）與輝達執行長（右）">
        <figcaption>台積電與輝達合作照。聯合報系資料照</figcaption>
    </figure>
    <p>預計下季度營收將持續成長。</p>
</div>
</body>
</html>
"""

SAMPLE_ARTICLE_HTML_WITH_HERO = """
<html>
<body>
<figure class="article-image">
    <a data-fancybox="gallery" href="https://pgw.udn.com.tw/gw/photo.php?u=test_hero.jpg">
        <picture>
            <img src="https://pgw.udn.com.tw/gw/photo.php?u=test_hero.jpg&w=441"
                 alt="主圖">
        </picture>
    </a>
    <figcaption>這是主圖圖說。聯合報系資料照</figcaption>
</figure>
<section id="article_body" class="article-body__editor">
    <p>文章內容第一段。</p>
    <p>文章內容第二段。</p>
</section>
</body>
</html>
"""

SAMPLE_ARTICLE_HTML_HERO_ONLY = """
<html>
<body>
<figure class="article-image">
    <img src="https://pgw.udn.com.tw/gw/photo.php?u=hero_only.jpg"
         alt="主圖">
    <figcaption>僅有主圖</figcaption>
</figure>
<div class="other-content">
    <p>這不是文章內容。</p>
</div>
</body>
</html>
"""

SAMPLE_ARTICLE_HTML_NO_BODY = """
<html>
<body>
<div class="other-content">
    <p>這不是文章內容。</p>
</div>
</body>
</html>
"""


# --- 單元測試：工具函式 ---


def test_create_session() -> None:
    """測試 session 建立包含正確的 headers。"""
    session = _create_session()
    assert "User-Agent" in session.headers
    assert "Mozilla" in session.headers["User-Agent"]
    assert "Referer" in session.headers


def test_parse_published_date_iso8601_with_tz() -> None:
    """測試 ISO 8601 含時區日期解析。"""
    dt = _parse_published_date("2026-02-27T08:30:00+08:00")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 2
    assert dt.day == 27
    assert dt.hour == 8
    assert dt.minute == 30


def test_parse_published_date_iso8601_no_tz() -> None:
    """測試 ISO 8601 無時區日期解析。"""
    dt = _parse_published_date("2026-02-27T08:30:00")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 2
    assert dt.day == 27


def test_parse_published_date_simple_format() -> None:
    """測試簡化格式日期解析。"""
    dt = _parse_published_date("2026-02-27 20:30:31")
    assert dt is not None
    assert dt.hour == 20
    assert dt.minute == 30
    assert dt.second == 31


def test_parse_published_date_simple_no_seconds() -> None:
    """測試簡化格式無秒數日期解析。"""
    dt = _parse_published_date("2026-02-27 20:30")
    assert dt is not None
    assert dt.hour == 20
    assert dt.minute == 30


def test_parse_published_date_empty() -> None:
    """測試空字串日期解析。"""
    assert _parse_published_date("") is None
    assert _parse_published_date(None) is None


def test_parse_published_date_invalid() -> None:
    """測試無效格式日期解析。"""
    assert _parse_published_date("not a date") is None


def test_build_full_url_relative() -> None:
    """測試相對 URL 轉完整 URL。"""
    url = _build_full_url("/money/story/5612/9348922")
    assert url == "https://money.udn.com/money/story/5612/9348922"


def test_build_full_url_with_tracking() -> None:
    """測試去除追蹤參數。"""
    url = _build_full_url(
        "/money/story/5612/9348922?from=edn_hottestlist_rank"
    )
    assert url == "https://money.udn.com/money/story/5612/9348922"
    assert "from=" not in url


def test_build_full_url_absolute() -> None:
    """測試已完整的 URL 不重複加前綴。"""
    url = _build_full_url("https://money.udn.com/money/story/5612/9348922")
    assert url == "https://money.udn.com/money/story/5612/9348922"


def test_extract_author_dict() -> None:
    """測試從 dict 格式提取作者。"""
    article = {"author": {"@type": "Person", "name": "記者鐘惠玲"}}
    assert _extract_author(article) == "記者鐘惠玲"


def test_extract_author_list() -> None:
    """測試從 list 格式提取多位作者。"""
    article = {
        "author": [
            {"@type": "Person", "name": "記者A"},
            {"@type": "Person", "name": "記者B"},
        ]
    }
    assert _extract_author(article) == "記者A,記者B"


def test_extract_author_string() -> None:
    """測試從字串格式提取作者。"""
    article = {"author": "記者張三"}
    assert _extract_author(article) == "記者張三"


def test_extract_author_empty() -> None:
    """測試無作者時回傳空字串。"""
    assert _extract_author({}) == ""
    assert _extract_author({"author": None}) == ""
    assert _extract_author({"author": ""}) == ""


def test_gen_empty_df() -> None:
    """測試空 DataFrame 產生。"""
    df = _gen_empty_df()
    assert df.empty
    expected_columns = [
        "Date", "Time", "Author", "Head", "url", "Content",
    ]
    assert df.columns.tolist() == expected_columns


# --- 單元測試：_parse_list_page ---


def test_parse_list_page_normal() -> None:
    """測試正常列表頁 JSON-LD 解析。"""
    articles = _parse_list_page(SAMPLE_LIST_HTML)

    assert len(articles) == 3

    # 檢查第一篇
    assert articles[0]["name"] == "台積電法說會釋利多 外資連買三日"
    assert articles[0]["url"] == "/money/story/5612/9348922"
    assert articles[0]["datePublished"] == "2026-02-27T08:30:00+08:00"
    assert articles[0]["author"]["name"] == "記者鐘惠玲"

    # 檢查第三篇（不同日期）
    assert articles[2]["name"] == "昨日舊聞標題"
    assert articles[2]["datePublished"] == "2026-02-26T18:00:00+08:00"


def test_parse_list_page_no_jsonld() -> None:
    """測試列表頁無 JSON-LD 時回傳空清單。"""
    articles = _parse_list_page(SAMPLE_LIST_HTML_NO_JSONLD)
    assert articles == []


def test_parse_list_page_empty_itemlist() -> None:
    """測試列表頁 ItemList 為空時回傳空清單。"""
    articles = _parse_list_page(SAMPLE_LIST_HTML_EMPTY_ITEMLIST)
    assert articles == []


def test_parse_list_page_invalid_json() -> None:
    """測試 JSON-LD 內容格式錯誤時回傳空清單。"""
    html = """
    <html><head>
    <script type="application/ld+json">{ invalid json }</script>
    </head><body></body></html>
    """
    articles = _parse_list_page(html)
    assert articles == []


# --- 單元測試：_fetch_article_content ---


def test_fetch_article_content_success(mocker: MockerFixture) -> None:
    """測試文章內容提取成功（含圖片保留）。"""
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_ARTICLE_HTML
    mock_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_response

    content = _fetch_article_content(
        mock_session,
        "https://money.udn.com/money/story/5612/9348922",
    )

    # 應包含文章內文
    assert "台積電今日召開法說會" in content
    assert "外資連續三個交易日買超" in content
    assert "預計下季度營收將持續成長" in content

    # 應保留圖片為 Markdown 格式
    assert "![" in content
    assert "pgw.udn.com.tw" in content


def test_extract_hero_image_with_figure() -> None:
    """測試從 figure.article-image 提取主圖。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML_WITH_HERO, "lxml")
    result = _extract_hero_image(soup)
    assert "![" in result
    assert "test_hero.jpg" in result
    assert "這是主圖圖說" in result


def test_extract_hero_image_no_figure() -> None:
    """測試無 figure.article-image 時回傳空字串。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML, "lxml")
    result = _extract_hero_image(soup)
    assert result == ""


def test_fetch_article_content_with_hero(mocker: MockerFixture) -> None:
    """測試文章含主圖時，主圖放在內文最前面。"""
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_ARTICLE_HTML_WITH_HERO
    mock_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_response

    content = _fetch_article_content(
        mock_session,
        "https://money.udn.com/money/story/5612/9348922",
    )

    # 主圖應在最前面
    assert content.startswith("![")
    assert "test_hero.jpg" in content
    # 內文也要有
    assert "文章內容第一段" in content
    assert "文章內容第二段" in content


def test_fetch_article_content_hero_only_no_body(
    mocker: MockerFixture,
) -> None:
    """測試僅有主圖無 article_body 時仍回傳主圖。"""
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_ARTICLE_HTML_HERO_ONLY
    mock_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_response

    content = _fetch_article_content(
        mock_session,
        "https://money.udn.com/money/story/5612/9348922",
    )

    assert "hero_only.jpg" in content
    assert "僅有主圖" in content


def test_fetch_article_content_no_body(mocker: MockerFixture) -> None:
    """測試文章頁面無 article-body 且無主圖時回傳空字串。"""
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_ARTICLE_HTML_NO_BODY
    mock_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_response

    content = _fetch_article_content(
        mock_session,
        "https://money.udn.com/money/story/5612/9348922",
    )

    assert content == ""


def test_fetch_article_content_request_failure(
    mocker: MockerFixture,
) -> None:
    """測試 HTTP 請求失敗時拋出異常。"""
    mock_session = mocker.Mock()
    mock_session.get.side_effect = requests.RequestException("timeout")

    with pytest.raises(requests.RequestException):
        _fetch_article_content(
            mock_session,
            "https://money.udn.com/money/story/5612/9348922",
        )


# --- 單元測試：moneyudn_news_crawler ---


def test_moneyudn_news_crawler_success(mocker: MockerFixture) -> None:
    """測試完整爬蟲流程正常回傳。"""
    # Mock 列表頁
    mock_list_response = mocker.Mock()
    mock_list_response.text = SAMPLE_LIST_HTML
    mock_list_response.raise_for_status = mocker.Mock()

    # Mock 文章頁
    mock_article_response = mocker.Mock()
    mock_article_response.text = SAMPLE_ARTICLE_HTML
    mock_article_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.side_effect = [
        mock_list_response,       # 列表頁
        mock_article_response,    # 第 1 篇文章
        mock_article_response,    # 第 2 篇文章
    ]
    mock_session.headers = dict(DEFAULT_HEADERS)

    mocker.patch(
        "tw_crawler.moneyudn_news._create_session",
        return_value=mock_session,
    )
    mocker.patch("tw_crawler.moneyudn_news.time.sleep")
    mocker.patch("tw_crawler.moneyudn_news.random.uniform", return_value=1.5)

    df = moneyudn_news_crawler("2026-02-27")

    assert not df.empty
    assert len(df) == 2  # 只有 2 篇是 2026-02-27 的
    assert list(df.columns) == [
        "Date", "Time", "Author", "Head", "url", "Content",
    ]
    assert df.iloc[0]["Date"] == "2026-02-27"
    assert df.iloc[0]["Time"] == "08:30:00"
    assert df.iloc[0]["Author"] == "記者鐘惠玲"
    assert df.iloc[0]["Head"] == "台積電法說會釋利多 外資連買三日"
    assert "money.udn.com" in df.iloc[0]["url"]
    assert "台積電" in df.iloc[0]["Content"]


def test_moneyudn_news_crawler_no_articles(mocker: MockerFixture) -> None:
    """測試無符合日期文章時回傳空 DataFrame。"""
    mock_list_response = mocker.Mock()
    mock_list_response.text = SAMPLE_LIST_HTML
    mock_list_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_list_response
    mock_session.headers = dict(DEFAULT_HEADERS)

    mocker.patch(
        "tw_crawler.moneyudn_news._create_session",
        return_value=mock_session,
    )

    df = moneyudn_news_crawler("2026-03-15")

    assert df.empty
    expected_columns = [
        "Date", "Time", "Author", "Head", "url", "Content",
    ]
    assert df.columns.tolist() == expected_columns


def test_moneyudn_news_crawler_list_request_failure(
    mocker: MockerFixture,
) -> None:
    """測試列表頁請求失敗時回傳空 DataFrame。"""
    mock_session = mocker.Mock()
    mock_session.get.side_effect = requests.RequestException("Connection error")
    mock_session.headers = dict(DEFAULT_HEADERS)

    mocker.patch(
        "tw_crawler.moneyudn_news._create_session",
        return_value=mock_session,
    )

    df = moneyudn_news_crawler("2026-02-27")

    assert df.empty


def test_moneyudn_news_crawler_empty_list(mocker: MockerFixture) -> None:
    """測試列表頁無 JSON-LD 資料時回傳空 DataFrame。"""
    mock_list_response = mocker.Mock()
    mock_list_response.text = SAMPLE_LIST_HTML_NO_JSONLD
    mock_list_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_list_response
    mock_session.headers = dict(DEFAULT_HEADERS)

    mocker.patch(
        "tw_crawler.moneyudn_news._create_session",
        return_value=mock_session,
    )

    df = moneyudn_news_crawler("2026-02-27")

    assert df.empty


def test_moneyudn_news_crawler_article_failure_continues(
    mocker: MockerFixture,
) -> None:
    """測試單篇文章爬取失敗不影響其他文章。"""
    mock_list_response = mocker.Mock()
    mock_list_response.text = SAMPLE_LIST_HTML
    mock_list_response.raise_for_status = mocker.Mock()

    mock_article_response = mocker.Mock()
    mock_article_response.text = SAMPLE_ARTICLE_HTML
    mock_article_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    # 列表頁成功，第一篇文章失敗，第二篇文章成功
    mock_session.get.side_effect = [
        mock_list_response,
        Exception("timeout"),
        mock_article_response,
    ]
    mock_session.headers = dict(DEFAULT_HEADERS)

    mocker.patch(
        "tw_crawler.moneyudn_news._create_session",
        return_value=mock_session,
    )
    mocker.patch("tw_crawler.moneyudn_news.time.sleep")
    mocker.patch("tw_crawler.moneyudn_news.random.uniform", return_value=1.5)

    df = moneyudn_news_crawler("2026-02-27")

    # 第一篇失敗，第二篇成功，應有 1 篇
    assert len(df) == 1
    assert df.iloc[0]["Head"] == "聯發科新晶片量產在即"


def test_moneyudn_news_crawler_date_filter() -> None:
    """測試日期過濾邏輯：只保留目標日期的文章。"""
    articles = _parse_list_page(SAMPLE_LIST_HTML)

    # 驗證解析結果
    assert len(articles) == 3

    # 模擬日期篩選邏輯
    from datetime import datetime
    target_date = datetime.strptime("2026-02-27", "%Y-%m-%d").date()

    matched = []
    for article in articles:
        dt = _parse_published_date(article.get("datePublished", ""))
        if dt and dt.date() == target_date:
            matched.append(article)

    # 2026-02-27 的文章只有 2 篇，2026-02-26 的 1 篇被排除
    assert len(matched) == 2
    assert matched[0]["name"] == "台積電法說會釋利多 外資連買三日"
    assert matched[1]["name"] == "聯發科新晶片量產在即"


# --- 單元測試：_collect_candidates_from_pages（分頁） ---


# 第二頁 HTML：含目標日期文章及更舊的文章
SAMPLE_LIST_HTML_PAGE2 = """
<html>
<head>
<script type="application/ld+json">
{
    "@context": "https://schema.org",
    "@graph": [
        {
            "@type": "ItemList",
            "itemListOrder": "ItemListOrderDescending",
            "numberOfItems": 2,
            "itemListElement": [
                {
                    "@type": "ListItem",
                    "position": 1,
                    "item": {
                        "@type": "NewsArticle",
                        "url": "/money/story/5612/9348800",
                        "name": "第二頁第一篇文章",
                        "datePublished": "2026-02-27T06:00:00+08:00",
                        "author": {"@type": "Person", "name": "記者王五"}
                    }
                },
                {
                    "@type": "ListItem",
                    "position": 2,
                    "item": {
                        "@type": "NewsArticle",
                        "url": "/money/story/5612/9348700",
                        "name": "前一天的舊文章",
                        "datePublished": "2026-02-26T12:00:00+08:00",
                        "author": {"@type": "Person", "name": "記者趙六"}
                    }
                }
            ]
        }
    ]
}
</script>
</head>
<body></body>
</html>
"""


def test_collect_candidates_multi_page(mocker: MockerFixture) -> None:
    """測試分頁收集：跨兩頁收集目標日期文章。"""
    from datetime import datetime

    mock_page1 = mocker.Mock()
    mock_page1.text = SAMPLE_LIST_HTML  # 2 篇 02-27 + 1 篇 02-26
    mock_page1.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_page1

    mocker.patch("tw_crawler.moneyudn_news.time.sleep")
    mocker.patch("tw_crawler.moneyudn_news.random.uniform", return_value=1.5)

    target = datetime.strptime("2026-02-27", "%Y-%m-%d").date()
    candidates = _collect_candidates_from_pages(mock_session, target)

    # 第一頁有 2 篇 02-27 + 1 篇 02-26（觸發停止），只需翻 1 頁
    assert len(candidates) == 2
    assert candidates[0]["name"] == "台積電法說會釋利多 外資連買三日"
    assert candidates[1]["name"] == "聯發科新晶片量產在即"
    # 只呼叫了 1 次 session.get（第 1 頁）
    assert mock_session.get.call_count == 1


def test_collect_candidates_needs_page2(mocker: MockerFixture) -> None:
    """測試分頁收集：第一頁全部是目標日期，需翻第二頁。"""
    from datetime import datetime

    # 第一頁：只有目標日期文章（沒有更舊的 → 需要翻頁）
    page1_html = """
    <html><head>
    <script type="application/ld+json">
    {"@graph": [{"@type": "ItemList", "itemListElement": [
        {"@type": "ListItem", "item": {
            "@type": "NewsArticle",
            "url": "/money/story/5612/9348922",
            "name": "第一頁文章A",
            "datePublished": "2026-02-27T20:00:00+08:00",
            "author": {"@type": "Person", "name": "記者A"}
        }},
        {"@type": "ListItem", "item": {
            "@type": "NewsArticle",
            "url": "/money/story/5612/9348921",
            "name": "第一頁文章B",
            "datePublished": "2026-02-27T18:00:00+08:00",
            "author": {"@type": "Person", "name": "記者B"}
        }}
    ]}]}
    </script></head><body></body></html>
    """
    mock_page1 = mocker.Mock()
    mock_page1.text = page1_html
    mock_page1.raise_for_status = mocker.Mock()

    mock_page2 = mocker.Mock()
    mock_page2.text = SAMPLE_LIST_HTML_PAGE2  # 1 篇 02-27 + 1 篇 02-26
    mock_page2.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.side_effect = [mock_page1, mock_page2]

    mocker.patch("tw_crawler.moneyudn_news.time.sleep")
    mocker.patch("tw_crawler.moneyudn_news.random.uniform", return_value=1.5)

    target = datetime.strptime("2026-02-27", "%Y-%m-%d").date()
    candidates = _collect_candidates_from_pages(mock_session, target)

    # 第一頁 2 篇 + 第二頁 1 篇 = 3 篇
    assert len(candidates) == 3
    assert candidates[0]["name"] == "第一頁文章A"
    assert candidates[1]["name"] == "第一頁文章B"
    assert candidates[2]["name"] == "第二頁第一篇文章"
    # 呼叫了 2 次 session.get（第 1 頁 + 第 2 頁）
    assert mock_session.get.call_count == 2


def test_collect_candidates_dedup_urls(mocker: MockerFixture) -> None:
    """測試分頁收集：重複 URL 自動去重。"""
    from datetime import datetime

    mock_page = mocker.Mock()
    mock_page.text = SAMPLE_LIST_HTML
    mock_page.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_page

    mocker.patch("tw_crawler.moneyudn_news.time.sleep")
    mocker.patch("tw_crawler.moneyudn_news.random.uniform", return_value=1.5)

    target = datetime.strptime("2026-02-27", "%Y-%m-%d").date()
    candidates = _collect_candidates_from_pages(mock_session, target)

    # 即使重複呼叫，URL 相同的文章不會重複
    urls = [c["url"] for c in candidates]
    assert len(urls) == len(set(urls))


def test_max_list_pages_constant() -> None:
    """測試 MAX_LIST_PAGES 常數存在且為合理數值。"""
    assert MAX_LIST_PAGES >= 1
    assert MAX_LIST_PAGES <= 20
