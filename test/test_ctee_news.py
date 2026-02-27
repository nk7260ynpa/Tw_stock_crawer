"""CTEE 工商時報股市新聞爬蟲測試模組。"""

import pandas as pd
from pytest_mock import MockerFixture

from tw_crawler.ctee_news import (
    _extract_author,
    _extract_content,
    _extract_hashtags,
    _extract_subtitle,
    _extract_time,
    _extract_title,
    _gen_empty_df,
    _parse_date_string,
    ctee_news_crawler,
    fetch_article_page,
    fetch_list_page,
    parse_article,
    parse_article_links,
)

# --- 測試用 HTML 模板 ---

SAMPLE_LIST_HTML = """
<html>
<body>
<div class="main-content">
    <article class="post-item">
        <h2><a href="https://www.ctee.com.tw/news/stock/1001.html">
            台積電營收創新高
        </a></h2>
        <time datetime="2024-10-15T08:30:00+08:00">2024-10-15</time>
    </article>
    <article class="post-item">
        <h2><a href="https://www.ctee.com.tw/news/stock/1002.html">
            聯發科法說會前瞻
        </a></h2>
        <time datetime="2024-10-15T10:00:00+08:00">2024-10-15</time>
    </article>
    <article class="post-item">
        <h2><a href="https://www.ctee.com.tw/news/stock/1000.html">
            舊聞標題
        </a></h2>
        <time datetime="2024-10-14T18:00:00+08:00">2024-10-14</time>
    </article>
</div>
</body>
</html>
"""

SAMPLE_LIST_HTML_NO_DATE = """
<html>
<body>
<div class="main-content">
    <article class="post-item">
        <h2><a href="https://www.ctee.com.tw/news/stock/2001.html">
            無日期的文章
        </a></h2>
    </article>
</div>
</body>
</html>
"""

SAMPLE_ARTICLE_HTML = """
<html>
<head>
    <meta property="og:title" content="台積電營收創新高 AI需求強勁" />
    <meta property="og:description" content="台積電第三季營收表現亮眼" />
    <meta name="author" content="記者王小明" />
    <meta property="article:tag" content="台積電" />
    <meta property="article:tag" content="AI" />
    <meta property="article:tag" content="半導體" />
</head>
<body>
<article>
    <header class="entry-header">
        <h1 class="entry-title">台積電營收創新高 AI需求強勁</h1>
        <div class="entry-subtitle">第三季營收年增逾三成</div>
    </header>
    <div class="post-meta">
        <span class="author"><a href="/author/wang">記者王小明</a></span>
        <time datetime="2024-10-15T08:30:00+08:00">2024/10/15 08:30</time>
    </div>
    <div class="entry-content clearfix single-post-content">
        <p>台積電今日公布第三季營收數據，受惠於AI晶片需求持續強勁。</p>
        <p>法人預估第四季營收有望再創新高。</p>
        <p>分析師指出，台積電在先進製程的領先優勢將持續帶動獲利成長。</p>
    </div>
    <div class="post-tags">
        <a href="/tag/tsmc" rel="tag">台積電</a>
        <a href="/tag/ai" rel="tag">AI</a>
        <a href="/tag/semiconductor" rel="tag">半導體</a>
    </div>
</article>
</body>
</html>
"""

SAMPLE_ARTICLE_HTML_MINIMAL = """
<html>
<head>
    <meta property="og:title" content="簡單標題" />
    <meta property="og:description" content="簡單描述" />
</head>
<body>
<article>
    <h1>簡單標題</h1>
    <div class="entry-content">
        <p>這是內文第一段。</p>
        <p>這是內文第二段。</p>
    </div>
</article>
</body>
</html>
"""


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


def test_parse_article_links() -> None:
    """測試從列表頁 HTML 解析文章連結。"""
    articles, found_older = parse_article_links(
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


def test_parse_article_links_no_match() -> None:
    """測試列表頁無符合日期的文章。"""
    articles, found_older = parse_article_links(
        SAMPLE_LIST_HTML, "2024-10-20"
    )
    assert len(articles) == 0
    assert found_older is True


def test_parse_article_links_no_date_in_element() -> None:
    """測試列表頁文章無日期資訊時仍收集連結。"""
    articles, _ = parse_article_links(
        SAMPLE_LIST_HTML_NO_DATE, "2024-10-15"
    )
    assert len(articles) == 1
    assert "2001.html" in articles[0]["url"]


def test_extract_title() -> None:
    """測試從文章頁面提取標題。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML, "lxml")
    title = _extract_title(soup)
    assert title == "台積電營收創新高 AI需求強勁"


def test_extract_title_fallback() -> None:
    """測試標題提取的備選機制（直接找 h1）。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML_MINIMAL, "lxml")
    title = _extract_title(soup)
    assert title == "簡單標題"


def test_extract_subtitle() -> None:
    """測試從文章頁面提取副標題。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML, "lxml")
    subtitle = _extract_subtitle(soup)
    assert subtitle == "第三季營收年增逾三成"


def test_extract_subtitle_fallback() -> None:
    """測試副標題提取的備選機制（og:description）。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML_MINIMAL, "lxml")
    subtitle = _extract_subtitle(soup)
    assert subtitle == "簡單描述"


def test_extract_author() -> None:
    """測試從文章頁面提取作者。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML, "lxml")
    author = _extract_author(soup)
    assert author == "記者王小明"


def test_extract_time() -> None:
    """測試從文章頁面提取時間。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML, "lxml")
    pub_time = _extract_time(soup)
    assert "2024-10-15" in pub_time


def test_extract_hashtags() -> None:
    """測試從文章頁面提取標籤。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML, "lxml")
    hashtags = _extract_hashtags(soup)
    assert "台積電" in hashtags
    assert "AI" in hashtags
    assert "半導體" in hashtags


def test_extract_hashtags_from_meta() -> None:
    """測試從 meta 標籤提取標籤。"""
    from bs4 import BeautifulSoup
    html = """
    <html>
    <head>
        <meta property="article:tag" content="科技" />
        <meta property="article:tag" content="投資" />
    </head>
    <body></body>
    </html>
    """
    soup = BeautifulSoup(html, "lxml")
    hashtags = _extract_hashtags(soup)
    assert "科技" in hashtags
    assert "投資" in hashtags


def test_extract_content() -> None:
    """測試從文章頁面提取內文。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML, "lxml")
    content = _extract_content(soup)
    assert "AI晶片需求" in content
    assert "第四季營收" in content
    assert "先進製程" in content


def test_extract_content_fallback() -> None:
    """測試內文提取的備選機制（div.entry-content）。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML_MINIMAL, "lxml")
    content = _extract_content(soup)
    assert "內文第一段" in content
    assert "內文第二段" in content


def test_parse_article() -> None:
    """測試完整文章解析。"""
    result = parse_article(
        SAMPLE_ARTICLE_HTML,
        "https://www.ctee.com.tw/news/stock/1001.html",
    )
    assert result["Head"] == "台積電營收創新高 AI需求強勁"
    assert result["SubHead"] == "第三季營收年增逾三成"
    assert result["Author"] == "記者王小明"
    assert "2024-10-15" in result["Time"]
    assert "台積電" in result["HashTag"]
    assert "AI晶片需求" in result["Content"]
    assert result["url"] == (
        "https://www.ctee.com.tw/news/stock/1001.html"
    )


def test_gen_empty_df() -> None:
    """測試空 DataFrame 產生。"""
    df = _gen_empty_df()
    assert df.empty
    expected_columns = [
        "Date", "Time", "Author", "Head", "SubHead",
        "HashTag", "url", "Content",
    ]
    assert df.columns.tolist() == expected_columns


def test_fetch_list_page(mocker: MockerFixture) -> None:
    """測試列表頁 HTTP 請求。"""
    mock_scraper = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_LIST_HTML
    mock_response.raise_for_status = mocker.Mock()
    mock_scraper.get.return_value = mock_response

    result = fetch_list_page(mock_scraper, page=1)
    assert result == SAMPLE_LIST_HTML
    mock_scraper.get.assert_called_once_with(
        "https://www.ctee.com.tw/stock/twmarket"
    )


def test_fetch_list_page_paged(mocker: MockerFixture) -> None:
    """測試分頁列表頁 HTTP 請求。"""
    mock_scraper = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_LIST_HTML
    mock_response.raise_for_status = mocker.Mock()
    mock_scraper.get.return_value = mock_response

    result = fetch_list_page(mock_scraper, page=3)
    assert result == SAMPLE_LIST_HTML
    mock_scraper.get.assert_called_once_with(
        "https://www.ctee.com.tw/stock/twmarket/page/3"
    )


def test_fetch_article_page(mocker: MockerFixture) -> None:
    """測試文章頁面 HTTP 請求。"""
    mock_scraper = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_ARTICLE_HTML
    mock_response.raise_for_status = mocker.Mock()
    mock_scraper.get.return_value = mock_response

    url = "https://www.ctee.com.tw/news/stock/1001.html"
    result = fetch_article_page(mock_scraper, url)
    assert result == SAMPLE_ARTICLE_HTML
    mock_scraper.get.assert_called_once_with(url)


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

    mock_scraper.get.side_effect = [
        list_response,      # 列表頁第 1 頁
        article_response,   # 文章 1
        article_response,   # 文章 2
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

    mock_scraper.get.return_value = list_response

    df = ctee_news_crawler("2024-12-25")

    assert df.empty
    expected_columns = [
        "Date", "Time", "Author", "Head", "SubHead",
        "HashTag", "url", "Content",
    ]
    assert df.columns.tolist() == expected_columns


def test_ctee_news_crawler_fetch_error(mocker: MockerFixture) -> None:
    """測試列表頁請求失敗時回傳空 DataFrame。"""
    mocker.patch("tw_crawler.ctee_news.time.sleep")
    mock_scraper = mocker.Mock()
    mocker.patch(
        "tw_crawler.ctee_news._create_scraper",
        return_value=mock_scraper,
    )

    mock_scraper.get.side_effect = Exception("Connection error")

    df = ctee_news_crawler("2024-10-15")

    assert df.empty
