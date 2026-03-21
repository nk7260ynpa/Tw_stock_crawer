"""PTT 股版新聞爬蟲測試模組。"""

import pandas as pd
import pytest
from bs4 import BeautifulSoup
from pytest_mock import MockerFixture

from tw_crawler.ptt_news import (
    MAX_RETRIES,
    PTT_STOCK_INDEX_URL,
    RETRY_BASE_DELAY,
    _create_session,
    _extract_article_content,
    _gen_empty_df,
    _parse_article_time,
    _parse_list_date,
    _request_with_retry,
    fetch_article_detail,
    fetch_list_page,
    parse_list_articles,
    ptt_news_crawler,
)

# --- 測試用 HTML 資料 ---

SAMPLE_LIST_HTML = """
<html>
<body>
<div class="btn-group-paging">
    <a href="/bbs/stock/index3999.html">上頁</a>
</div>
<div class="r-bar"></div>
<div class="r-ent">
    <div class="nrec"><span class="hl f3">10</span></div>
    <div class="title"><a href="/bbs/stock/M.1740652200.A.B01.html">
    [新聞] 台積電法說會釋利多</a></div>
    <div class="meta">
        <div class="author">stockman</div>
        <div class="date"> 2/27</div>
    </div>
</div>
<div class="r-ent">
    <div class="nrec"><span class="hl f2">5</span></div>
    <div class="title"><a href="/bbs/stock/M.1740652300.A.C02.html">
    [閒聊] 今天大盤走勢</a></div>
    <div class="meta">
        <div class="author">trader99</div>
        <div class="date"> 2/27</div>
    </div>
</div>
<div class="r-ent">
    <div class="nrec"></div>
    <div class="title">(本文已被刪除) [stockman]</div>
    <div class="meta">
        <div class="author">-</div>
        <div class="date"> 2/27</div>
    </div>
</div>
<div class="r-ent">
    <div class="nrec"><span class="hl f1">3</span></div>
    <div class="title"><a href="/bbs/stock/M.1740565800.A.A01.html">
    [新聞] 昨日舊聞</a></div>
    <div class="meta">
        <div class="author">oldnews</div>
        <div class="date"> 2/26</div>
    </div>
</div>
</body>
</html>
"""

SAMPLE_LIST_HTML_NO_PREV = """
<html>
<body>
<div class="btn-group-paging">
    <a href="/bbs/stock/index1.html">最舊</a>
</div>
<div class="r-ent">
    <div class="nrec"></div>
    <div class="title"><a href="/bbs/stock/M.1740652200.A.B01.html">
    [新聞] 第一篇文章</a></div>
    <div class="meta">
        <div class="author">firstpost</div>
        <div class="date"> 2/27</div>
    </div>
</div>
</body>
</html>
"""

SAMPLE_ARTICLE_HTML = """
<html>
<body>
<div id="main-content" class="bbs-screen bbs-content">
    <div class="article-metaline">
        <span class="article-meta-tag">作者</span>
        <span class="article-meta-value">stockman (股票達人)</span>
    </div>
    <div class="article-metaline">
        <span class="article-meta-tag">標題</span>
        <span class="article-meta-value">[新聞] 台積電法說會釋利多</span>
    </div>
    <div class="article-metaline">
        <span class="article-meta-tag">時間</span>
        <span class="article-meta-value">Fri Feb 27 14:30:00 2026</span>
    </div>

台積電今日召開法說會，釋出正面展望。
外資連續三個交易日買超。

預計下季度營收將持續成長。

※ 發信站: 批踢踢實業坊(ptt.cc), 來自: 1.2.3.4
※ 文章網址: https://www.ptt.cc/bbs/stock/M.1740652200.A.B01.html
    <div class="push">
        <span class="push-tag">推 </span>
        <span class="push-userid">user1</span>
        <span class="push-content">: 利多!</span>
    </div>
    <div class="push">
        <span class="push-tag">推 </span>
        <span class="push-userid">user2</span>
        <span class="push-content">: 讚</span>
    </div>
</div>
</body>
</html>
"""

SAMPLE_LIST_HTML_WITH_SEPARATOR = """
<html>
<body>
<div class="btn-group-paging">
    <a href="/bbs/stock/index3999.html">上頁</a>
</div>
<div class="r-list-container action-bar-margin bbs-screen">
<div class="r-ent">
    <div class="nrec"><span class="hl f3">10</span></div>
    <div class="title"><a href="/bbs/stock/M.1742572800.A.B01.html">
    [新聞] 台積電法說會釋利多</a></div>
    <div class="meta">
        <div class="author">stockman</div>
        <div class="date"> 3/21</div>
    </div>
</div>
<div class="r-ent">
    <div class="nrec"><span class="hl f2">5</span></div>
    <div class="title"><a href="/bbs/stock/M.1742572900.A.C02.html">
    [閒聊] 今天大盤走勢</a></div>
    <div class="meta">
        <div class="author">trader99</div>
        <div class="date"> 3/21</div>
    </div>
</div>
<div class="r-ent">
    <div class="nrec"><span class="hl f1">3</span></div>
    <div class="title"><a href="/bbs/stock/M.1742486400.A.A01.html">
    [新聞] 昨日舊聞</a></div>
    <div class="meta">
        <div class="author">oldnews</div>
        <div class="date"> 3/20</div>
    </div>
</div>
<div class="r-list-sep"></div>
<div class="r-ent">
    <div class="nrec"><span class="hl f3">99</span></div>
    <div class="title"><a href="/bbs/stock/M.1719878400.A.D01.html">
    [公告] 股票板板規 v4.7</a></div>
    <div class="meta">
        <div class="author">modstock</div>
        <div class="date"> 7/02</div>
    </div>
</div>
<div class="r-ent">
    <div class="nrec"><span class="hl f3">50</span></div>
    <div class="title"><a href="/bbs/stock/M.1737417600.A.E01.html">
    [公告] 4-6-1罰則</a></div>
    <div class="meta">
        <div class="author">modstock</div>
        <div class="date"> 1/21</div>
    </div>
</div>
<div class="r-ent">
    <div class="nrec"><span class="hl f2">20</span></div>
    <div class="title"><a href="/bbs/stock/M.1742486400.A.F01.html">
    [閒聊] 盤後閒聊</a></div>
    <div class="meta">
        <div class="author">modstock</div>
        <div class="date"> 3/20</div>
    </div>
</div>
</div>
</body>
</html>
"""

SAMPLE_LIST_HTML_NO_SEPARATOR = """
<html>
<body>
<div class="btn-group-paging">
    <a href="/bbs/stock/index3998.html">上頁</a>
</div>
<div class="r-list-container action-bar-margin bbs-screen">
<div class="r-ent">
    <div class="nrec"><span class="hl f3">8</span></div>
    <div class="title"><a href="/bbs/stock/M.1742486400.A.G01.html">
    [新聞] 聯發科營收創高</a></div>
    <div class="meta">
        <div class="author">chipfan</div>
        <div class="date"> 3/20</div>
    </div>
</div>
<div class="r-ent">
    <div class="nrec"><span class="hl f1">2</span></div>
    <div class="title"><a href="/bbs/stock/M.1742400000.A.H01.html">
    [新聞] 鴻海布局AI</a></div>
    <div class="meta">
        <div class="author">techguy</div>
        <div class="date"> 3/19</div>
    </div>
</div>
</div>
</body>
</html>
"""

SAMPLE_ARTICLE_HTML_NO_META = """
<html>
<body>
<div id="main-content" class="bbs-screen bbs-content">
文章內容但缺少 metaline。
</div>
</body>
</html>
"""


# --- 單元測試：工具函式 ---


def test_parse_list_date_normal() -> None:
    """測試列表頁日期解析（一般格式）。"""
    dt = _parse_list_date("2/27", 2026)
    assert dt is not None
    assert dt.month == 2
    assert dt.day == 27
    assert dt.year == 2026


def test_parse_list_date_with_leading_space() -> None:
    """測試列表頁日期解析（前導空白）。"""
    dt = _parse_list_date(" 2/27", 2026)
    assert dt is not None
    assert dt.month == 2
    assert dt.day == 27


def test_parse_list_date_double_digit() -> None:
    """測試列表頁日期解析（雙位數月日）。"""
    dt = _parse_list_date("12/31", 2025)
    assert dt is not None
    assert dt.month == 12
    assert dt.day == 31


def test_parse_list_date_empty() -> None:
    """測試列表頁日期解析（空字串）。"""
    assert _parse_list_date("", 2026) is None


def test_parse_list_date_invalid() -> None:
    """測試列表頁日期解析（無效格式）。"""
    assert _parse_list_date("abc", 2026) is None


def test_parse_list_date_invalid_date() -> None:
    """測試列表頁日期解析（不存在的日期）。"""
    assert _parse_list_date("2/30", 2026) is None


def test_parse_article_time_normal() -> None:
    """測試文章時間解析（正常格式）。"""
    dt = _parse_article_time("Fri Feb 27 14:30:00 2026")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 2
    assert dt.day == 27
    assert dt.hour == 14
    assert dt.minute == 30
    assert dt.second == 0


def test_parse_article_time_empty() -> None:
    """測試文章時間解析（空字串）。"""
    assert _parse_article_time("") is None


def test_parse_article_time_invalid() -> None:
    """測試文章時間解析（無效格式）。"""
    assert _parse_article_time("not a date") is None


def test_extract_article_content() -> None:
    """測試文章全文提取。"""
    soup = BeautifulSoup(SAMPLE_ARTICLE_HTML, "lxml")
    content = _extract_article_content(soup)

    # 應包含文章內文
    assert "台積電今日召開法說會" in content
    assert "外資連續三個交易日買超" in content

    # 不應包含推文
    assert "利多!" not in content
    assert "user1" not in content

    # 不應包含發信站資訊
    assert "發信站" not in content


def test_extract_article_content_no_main() -> None:
    """測試無 main-content 時回傳空字串。"""
    soup = BeautifulSoup("<html><body></body></html>", "lxml")
    content = _extract_article_content(soup)
    assert content == ""


def test_gen_empty_df() -> None:
    """測試空 DataFrame 產生。"""
    df = _gen_empty_df()
    assert df.empty
    expected_columns = [
        "Date", "Time", "Author", "Head", "url", "Content",
    ]
    assert df.columns.tolist() == expected_columns


def test_create_session() -> None:
    """測試 session 建立包含 over18 cookie。"""
    session = _create_session()
    assert session.cookies.get("over18") == "1"


# --- 單元測試：parse_list_articles ---


def test_parse_list_articles_filter_by_date() -> None:
    """測試日期篩選：只保留目標日期的文章。"""
    soup = BeautifulSoup(SAMPLE_LIST_HTML, "lxml")
    matched, found_older = parse_list_articles(soup, "2026-02-27", 2026)

    # 應匹配 2 篇（2/27 的文章），跳過已刪除文章，排除 2/26 的舊聞
    assert len(matched) == 2
    assert found_older is True

    # 檢查第一篇
    assert matched[0]["title"] == "[新聞] 台積電法說會釋利多"
    assert matched[0]["author"] == "stockman"
    assert "M.1740652200" in matched[0]["url"]

    # 檢查第二篇
    assert matched[1]["title"] == "[閒聊] 今天大盤走勢"
    assert matched[1]["author"] == "trader99"


def test_parse_list_articles_skip_deleted() -> None:
    """測試跳過已刪除文章（無 <a> 標籤）。"""
    soup = BeautifulSoup(SAMPLE_LIST_HTML, "lxml")
    matched, _ = parse_list_articles(soup, "2026-02-27", 2026)

    # 已刪除文章不應出現在結果中
    for article in matched:
        assert article["author"] != "-"
        assert "(本文已被刪除)" not in article.get("title", "")


def test_parse_list_articles_no_match() -> None:
    """測試無符合日期的文章。"""
    soup = BeautifulSoup(SAMPLE_LIST_HTML, "lxml")
    matched, found_older = parse_list_articles(soup, "2026-03-01", 2026)

    assert len(matched) == 0
    assert found_older is True  # 所有文章都比 3/1 早


def test_parse_list_articles_all_match_no_older() -> None:
    """測試所有文章都在目標日期（無更早文章）。"""
    html = """
    <html><body>
    <div class="r-ent">
        <div class="title"><a href="/bbs/stock/M.123.A.B01.html">
        [新聞] 測試</a></div>
        <div class="meta">
            <div class="author">author1</div>
            <div class="date"> 2/27</div>
        </div>
    </div>
    </body></html>
    """
    soup = BeautifulSoup(html, "lxml")
    matched, found_older = parse_list_articles(soup, "2026-02-27", 2026)

    assert len(matched) == 1
    assert found_older is False


def test_parse_list_articles_skip_pinned_with_separator() -> None:
    """測試含分隔線的列表頁：只處理分隔線以上的正常文章。

    PTT 最新頁面有 div.r-list-sep 分隔線，以下為置頂公告。
    置頂公告日期可能很舊（如 7/02、1/21），不應觸發 found_older。
    """
    soup = BeautifulSoup(SAMPLE_LIST_HTML_WITH_SEPARATOR, "lxml")
    matched, found_older = parse_list_articles(soup, "2026-03-21", 2026)

    # 分隔線以上有 2 篇 3/21 + 1 篇 3/20，目標日期 3/21 應匹配 2 篇
    assert len(matched) == 2
    assert matched[0]["title"] == "[新聞] 台積電法說會釋利多"
    assert matched[0]["author"] == "stockman"
    assert matched[1]["title"] == "[閒聊] 今天大盤走勢"
    assert matched[1]["author"] == "trader99"

    # 分隔線以上有 3/20 的文章 → found_older = True
    assert found_older is True


def test_parse_list_articles_pinned_not_trigger_older() -> None:
    """測試置頂文章的舊日期不會誤觸 found_older。

    修復前的問題：置頂公告日期（如 7/02）被當成更早文章，
    導致爬蟲在第一頁就停止翻頁。
    """
    soup = BeautifulSoup(SAMPLE_LIST_HTML_WITH_SEPARATOR, "lxml")

    # 目標日期 3/20：分隔線以上 3 篇都 >= 3/20，不應有更早文章
    matched, found_older = parse_list_articles(soup, "2026-03-20", 2026)

    # 應匹配 3/20 的 1 篇
    assert len(matched) == 1
    assert matched[0]["title"] == "[新聞] 昨日舊聞"

    # 重點：雖然置頂有 7/02、1/21 等舊日期，但被跳過不處理
    # 分隔線以上的文章都 >= 3/20，所以 found_older 應為 False
    assert found_older is False


def test_parse_list_articles_no_separator_page() -> None:
    """測試無分隔線的非最新頁面：處理所有文章（行為不變）。"""
    soup = BeautifulSoup(SAMPLE_LIST_HTML_NO_SEPARATOR, "lxml")
    matched, found_older = parse_list_articles(soup, "2026-03-20", 2026)

    # 應匹配 3/20 的 1 篇
    assert len(matched) == 1
    assert matched[0]["title"] == "[新聞] 聯發科營收創高"
    assert matched[0]["author"] == "chipfan"

    # 3/19 的文章 → found_older = True
    assert found_older is True


def test_parse_list_articles_separator_hours_mode() -> None:
    """測試含分隔線的列表頁在時數模式下也能正確過濾置頂文章。"""
    from datetime import date
    soup = BeautifulSoup(SAMPLE_LIST_HTML_WITH_SEPARATOR, "lxml")

    # cutoff_date = 3/21，分隔線以上只有 3/21 的 2 篇匹配
    matched, found_older = parse_list_articles(
        soup, "2026-03-21", 2026, cutoff_date=date(2026, 3, 21),
    )

    assert len(matched) == 2
    # 分隔線以上 3/20 < cutoff → found_older = True
    assert found_older is True


def test_parse_list_articles_no_container_fallback() -> None:
    """測試無 r-list-container 時退回原始 find_all 邏輯。"""
    # 使用不含 r-list-container 的舊格式 HTML
    soup = BeautifulSoup(SAMPLE_LIST_HTML, "lxml")
    matched, found_older = parse_list_articles(soup, "2026-02-27", 2026)

    # 原有行為不變：2 篇 2/27 + found_older (2/26)
    assert len(matched) == 2
    assert found_older is True


# --- 單元測試：fetch_list_page ---


def test_fetch_list_page_with_prev_link(mocker: MockerFixture) -> None:
    """測試列表頁取得含上頁連結。"""
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_LIST_HTML
    mock_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_response

    soup, prev_url = fetch_list_page(mock_session, PTT_STOCK_INDEX_URL)

    assert prev_url == "https://www.ptt.cc/bbs/stock/index3999.html"
    assert soup is not None
    mock_session.get.assert_called_once()


def test_fetch_list_page_no_prev_link(mocker: MockerFixture) -> None:
    """測試列表頁無上頁連結。"""
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_LIST_HTML_NO_PREV
    mock_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_response

    soup, prev_url = fetch_list_page(mock_session, PTT_STOCK_INDEX_URL)

    # 「最舊」不是「上頁」，應回傳 None
    assert prev_url is None


# --- 單元測試：fetch_article_detail ---


def test_fetch_article_detail_success(mocker: MockerFixture) -> None:
    """測試文章詳情取得成功。"""
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_ARTICLE_HTML
    mock_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_response

    result = fetch_article_detail(
        mock_session,
        "https://www.ptt.cc/bbs/stock/M.1740652200.A.B01.html",
    )

    assert result["time_str"] == "14:30:00"
    assert result["full_datetime"] is not None
    assert result["full_datetime"].year == 2026
    assert result["full_datetime"].month == 2
    assert result["full_datetime"].day == 27
    assert "台積電今日召開法說會" in result["content"]
    # 不應包含推文
    assert "利多" not in result["content"]


def test_fetch_article_detail_no_meta(mocker: MockerFixture) -> None:
    """測試文章缺少 metaline 時仍能取得內文。"""
    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_ARTICLE_HTML_NO_META
    mock_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_response

    result = fetch_article_detail(
        mock_session,
        "https://www.ptt.cc/bbs/stock/M.123.A.B01.html",
    )

    assert result["time_str"] == ""
    assert result["full_datetime"] is None
    assert "文章內容但缺少 metaline" in result["content"]


# --- 單元測試：ptt_news_crawler ---


def test_ptt_news_crawler_success(mocker: MockerFixture) -> None:
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
        mock_list_response,      # 列表頁
        mock_article_response,   # 第 1 篇文章
        mock_article_response,   # 第 2 篇文章
    ]
    mock_session.cookies = mocker.Mock()
    mock_session.cookies.get.return_value = "1"
    mock_session.headers = {}

    mocker.patch(
        "tw_crawler.ptt_news._create_session",
        return_value=mock_session,
    )
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    df = ptt_news_crawler("2026-02-27")

    assert not df.empty
    assert len(df) == 2
    assert list(df.columns) == [
        "Date", "Time", "Author", "Head", "url", "Content",
    ]
    assert df.iloc[0]["Date"] == "2026-02-27"
    assert df.iloc[0]["Time"] == "14:30:00"
    assert "台積電" in df.iloc[0]["Content"]


def test_ptt_news_crawler_no_articles(mocker: MockerFixture) -> None:
    """測試無符合日期文章時回傳空 DataFrame。"""
    mock_list_response = mocker.Mock()
    mock_list_response.text = SAMPLE_LIST_HTML
    mock_list_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_list_response
    mock_session.cookies = mocker.Mock()
    mock_session.cookies.get.return_value = "1"
    mock_session.headers = {}

    mocker.patch(
        "tw_crawler.ptt_news._create_session",
        return_value=mock_session,
    )
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    df = ptt_news_crawler("2026-03-15")

    assert df.empty
    expected_columns = [
        "Date", "Time", "Author", "Head", "url", "Content",
    ]
    assert df.columns.tolist() == expected_columns


def test_ptt_news_crawler_request_failure(mocker: MockerFixture) -> None:
    """測試 HTTP 請求失敗時回傳空 DataFrame。"""
    import requests as req

    mock_session = mocker.Mock()
    mock_session.get.side_effect = req.RequestException("Connection error")
    mock_session.cookies = mocker.Mock()
    mock_session.cookies.get.return_value = "1"
    mock_session.headers = {}

    mocker.patch(
        "tw_crawler.ptt_news._create_session",
        return_value=mock_session,
    )
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    df = ptt_news_crawler("2026-02-27")

    assert df.empty


def test_ptt_news_crawler_article_failure_continues(
    mocker: MockerFixture,
) -> None:
    """測試單篇文章爬取失敗不影響其他文章。"""
    # 只有一篇匹配的簡化列表
    simple_list_html = """
    <html><body>
    <div class="r-ent">
        <div class="title"><a href="/bbs/stock/M.1.A.B01.html">
        [新聞] 第一篇</a></div>
        <div class="meta">
            <div class="author">author1</div>
            <div class="date"> 2/27</div>
        </div>
    </div>
    <div class="r-ent">
        <div class="title"><a href="/bbs/stock/M.2.A.B01.html">
        [新聞] 第二篇</a></div>
        <div class="meta">
            <div class="author">author2</div>
            <div class="date"> 2/27</div>
        </div>
    </div>
    <div class="r-ent">
        <div class="title"><a href="/bbs/stock/M.3.A.B01.html">
        [新聞] 舊文</a></div>
        <div class="meta">
            <div class="author">old</div>
            <div class="date"> 2/26</div>
        </div>
    </div>
    </body></html>
    """
    mock_list_response = mocker.Mock()
    mock_list_response.text = simple_list_html
    mock_list_response.raise_for_status = mocker.Mock()

    mock_article_response = mocker.Mock()
    mock_article_response.text = SAMPLE_ARTICLE_HTML
    mock_article_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    # 第一次列表頁，第二次文章爬取失敗，第三次文章爬取成功
    mock_session.get.side_effect = [
        mock_list_response,
        Exception("timeout"),
        mock_article_response,
    ]
    mock_session.cookies = mocker.Mock()
    mock_session.cookies.get.return_value = "1"
    mock_session.headers = {}

    mocker.patch(
        "tw_crawler.ptt_news._create_session",
        return_value=mock_session,
    )
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    df = ptt_news_crawler("2026-02-27")

    # 應有 1 篇成功（第二篇），第一篇失敗被跳過
    assert len(df) == 1
    assert df.iloc[0]["Author"] == "author2"


def test_ptt_news_crawler_date_verify_skips_wrong_date(
    mocker: MockerFixture,
) -> None:
    """測試文章頁面日期與目標不符時跳過該文章。"""
    # 列表頁顯示 2/27，但文章頁面實際日期是 2/26
    list_html = """
    <html><body>
    <div class="r-ent">
        <div class="title"><a href="/bbs/stock/M.1.A.B01.html">
        [新聞] 跨日文章</a></div>
        <div class="meta">
            <div class="author">author1</div>
            <div class="date"> 2/27</div>
        </div>
    </div>
    <div class="r-ent">
        <div class="title"><a href="/bbs/stock/M.3.A.B01.html">
        [新聞] 舊文</a></div>
        <div class="meta">
            <div class="author">old</div>
            <div class="date"> 2/26</div>
        </div>
    </div>
    </body></html>
    """
    # 文章頁面的實際時間是 2/26（與列表頁的 2/27 不符）
    wrong_date_article = """
    <html><body>
    <div id="main-content">
        <div class="article-metaline">
            <span class="article-meta-tag">作者</span>
            <span class="article-meta-value">author1</span>
        </div>
        <div class="article-metaline">
            <span class="article-meta-tag">標題</span>
            <span class="article-meta-value">[新聞] 跨日文章</span>
        </div>
        <div class="article-metaline">
            <span class="article-meta-tag">時間</span>
            <span class="article-meta-value">Wed Feb 26 23:59:00 2026</span>
        </div>
    內容文字。
    </div>
    </body></html>
    """
    mock_list_response = mocker.Mock()
    mock_list_response.text = list_html
    mock_list_response.raise_for_status = mocker.Mock()

    mock_article_response = mocker.Mock()
    mock_article_response.text = wrong_date_article
    mock_article_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.side_effect = [
        mock_list_response,
        mock_article_response,
    ]
    mock_session.cookies = mocker.Mock()
    mock_session.cookies.get.return_value = "1"
    mock_session.headers = {}

    mocker.patch(
        "tw_crawler.ptt_news._create_session",
        return_value=mock_session,
    )
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    df = ptt_news_crawler("2026-02-27")

    # 文章實際日期 2/26 與目標 2/27 不符，應被跳過
    assert df.empty


# --- 單元測試：時數模式（hours） ---


def test_parse_list_articles_hours_mode() -> None:
    """測試時數模式篩選：cutoff_date 當日及之後的文章。"""
    from datetime import date
    soup = BeautifulSoup(SAMPLE_LIST_HTML, "lxml")

    # cutoff_date = 2026-02-27，應包含 2/27 的 2 篇，排除 2/26
    matched, found_older = parse_list_articles(
        soup, "2026-02-27", 2026, cutoff_date=date(2026, 2, 27),
    )

    assert len(matched) == 2
    assert found_older is True


def test_parse_list_articles_hours_mode_cross_day() -> None:
    """測試時數模式跨日：cutoff_date 在前一天。"""
    from datetime import date
    soup = BeautifulSoup(SAMPLE_LIST_HTML, "lxml")

    # cutoff_date = 2026-02-26，應包含所有 2/26 和 2/27 的文章
    matched, found_older = parse_list_articles(
        soup, "2026-02-27", 2026, cutoff_date=date(2026, 2, 26),
    )

    # 應包含 2/27 的 2 篇 + 2/26 的 1 篇 = 3 篇
    assert len(matched) == 3
    assert found_older is False


def test_ptt_news_crawler_hours_mode(mocker: MockerFixture) -> None:
    """測試完整爬蟲流程（時數模式）。"""
    from datetime import datetime as dt_cls, timezone, timedelta
    TW_TZ = timezone(timedelta(hours=8))

    # Mock datetime.now 回傳 2026-02-27 22:00 UTC+8
    mock_now = dt_cls(2026, 2, 27, 22, 0, 0, tzinfo=TW_TZ)
    mocker.patch(
        "tw_crawler.ptt_news.datetime",
        wraps=dt_cls,
    )
    mocker.patch(
        "tw_crawler.ptt_news.datetime.now",
        return_value=mock_now,
    )

    # Mock 列表頁
    mock_list_response = mocker.Mock()
    mock_list_response.text = SAMPLE_LIST_HTML
    mock_list_response.raise_for_status = mocker.Mock()

    # Mock 文章頁（時間是 2026-02-27 14:30，在 cutoff 之後）
    mock_article_response = mocker.Mock()
    mock_article_response.text = SAMPLE_ARTICLE_HTML
    mock_article_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.side_effect = [
        mock_list_response,      # 列表頁
        mock_article_response,   # 第 1 篇文章
        mock_article_response,   # 第 2 篇文章
    ]
    mock_session.cookies = mocker.Mock()
    mock_session.cookies.get.return_value = "1"
    mock_session.headers = {}

    mocker.patch(
        "tw_crawler.ptt_news._create_session",
        return_value=mock_session,
    )
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    # hours=24 -> cutoff = 2026-02-26 22:00 UTC+8
    df = ptt_news_crawler("2026-02-27", hours=24)

    assert not df.empty
    assert list(df.columns) == [
        "Date", "Time", "Author", "Head", "url", "Content",
    ]
    # 文章的 Date 應是文章自身的日期
    assert df.iloc[0]["Date"] == "2026-02-27"


def test_ptt_news_crawler_hours_mode_filters_old(
    mocker: MockerFixture,
) -> None:
    """測試時數模式精確篩選：文章實際時間早於 cutoff 被跳過。"""
    from datetime import datetime as dt_cls, timezone, timedelta
    TW_TZ = timezone(timedelta(hours=8))

    # Mock datetime.now 回傳 2026-02-27 15:00 UTC+8
    mock_now = dt_cls(2026, 2, 27, 15, 0, 0, tzinfo=TW_TZ)
    mocker.patch(
        "tw_crawler.ptt_news.datetime",
        wraps=dt_cls,
    )
    mocker.patch(
        "tw_crawler.ptt_news.datetime.now",
        return_value=mock_now,
    )

    # 簡化列表頁，只有一篇文章
    list_html = """
    <html><body>
    <div class="r-ent">
        <div class="title"><a href="/bbs/stock/M.1.A.B01.html">
        [新聞] 老文章</a></div>
        <div class="meta">
            <div class="author">author1</div>
            <div class="date"> 2/27</div>
        </div>
    </div>
    <div class="r-ent">
        <div class="title"><a href="/bbs/stock/M.2.A.B01.html">
        [新聞] 舊文</a></div>
        <div class="meta">
            <div class="author">old</div>
            <div class="date"> 2/26</div>
        </div>
    </div>
    </body></html>
    """
    # 文章頁面時間是 2026-02-27 06:00（cutoff=14:00，早於 cutoff）
    old_article = """
    <html><body>
    <div id="main-content">
        <div class="article-metaline">
            <span class="article-meta-tag">作者</span>
            <span class="article-meta-value">author1</span>
        </div>
        <div class="article-metaline">
            <span class="article-meta-tag">標題</span>
            <span class="article-meta-value">[新聞] 老文章</span>
        </div>
        <div class="article-metaline">
            <span class="article-meta-tag">時間</span>
            <span class="article-meta-value">Fri Feb 27 06:00:00 2026</span>
        </div>
    文章內容。
    </div>
    </body></html>
    """
    mock_list_response = mocker.Mock()
    mock_list_response.text = list_html
    mock_list_response.raise_for_status = mocker.Mock()

    mock_article_response = mocker.Mock()
    mock_article_response.text = old_article
    mock_article_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.side_effect = [
        mock_list_response,
        mock_article_response,
    ]
    mock_session.cookies = mocker.Mock()
    mock_session.cookies.get.return_value = "1"
    mock_session.headers = {}

    mocker.patch(
        "tw_crawler.ptt_news._create_session",
        return_value=mock_session,
    )
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    # hours=1 -> cutoff = 2026-02-27 14:00，文章 06:00 早於 cutoff
    df = ptt_news_crawler("2026-02-27", hours=1)

    assert df.empty


# --- 單元測試：_request_with_retry ---


def test_request_with_retry_success_first_attempt(
    mocker: MockerFixture,
) -> None:
    """測試首次請求即成功，不需重試。"""
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    mock_response = mocker.Mock()
    mock_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_response

    result = _request_with_retry(mock_session, "https://example.com", "測試")

    assert result is mock_response
    mock_session.get.assert_called_once()


def test_request_with_retry_success_after_failures(
    mocker: MockerFixture,
) -> None:
    """測試前兩次失敗、第三次成功的重試。"""
    mock_sleep = mocker.patch("tw_crawler.ptt_news.time.sleep")

    mock_response = mocker.Mock()
    mock_response.raise_for_status = mocker.Mock()

    import requests as req
    ssl_error = req.exceptions.SSLError("SSL: UNEXPECTED_EOF_WHILE_READING")

    mock_session = mocker.Mock()
    mock_session.get.side_effect = [
        ssl_error,       # 第 1 次失敗
        ssl_error,       # 第 2 次失敗
        mock_response,   # 第 3 次成功
    ]

    result = _request_with_retry(mock_session, "https://example.com", "列表頁")

    assert result is mock_response
    assert mock_session.get.call_count == 3
    # 驗證指數退避延遲：1s, 2s
    assert mock_sleep.call_count == 2
    mock_sleep.assert_any_call(RETRY_BASE_DELAY * 1)  # 1s
    mock_sleep.assert_any_call(RETRY_BASE_DELAY * 2)  # 2s


def test_request_with_retry_all_attempts_fail(
    mocker: MockerFixture,
) -> None:
    """測試所有重試皆失敗，應拋出最後的例外。"""
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    import requests as req
    ssl_error = req.exceptions.SSLError("SSL: UNEXPECTED_EOF_WHILE_READING")

    mock_session = mocker.Mock()
    mock_session.get.side_effect = ssl_error

    with pytest.raises(req.exceptions.SSLError, match="UNEXPECTED_EOF"):
        _request_with_retry(mock_session, "https://example.com", "文章")

    assert mock_session.get.call_count == MAX_RETRIES


def test_request_with_retry_connection_error(
    mocker: MockerFixture,
) -> None:
    """測試 ConnectionError 也會觸發重試。"""
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    mock_response = mocker.Mock()
    mock_response.raise_for_status = mocker.Mock()

    import requests as req
    conn_error = req.ConnectionError("Connection reset by peer")

    mock_session = mocker.Mock()
    mock_session.get.side_effect = [conn_error, mock_response]

    result = _request_with_retry(mock_session, "https://example.com", "列表頁")

    assert result is mock_response
    assert mock_session.get.call_count == 2


def test_fetch_list_page_retries_on_ssl_error(
    mocker: MockerFixture,
) -> None:
    """測試 fetch_list_page 遇到 SSL 錯誤自動重試成功。"""
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    import requests as req
    ssl_error = req.exceptions.SSLError("SSL EOF")

    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_LIST_HTML
    mock_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.side_effect = [ssl_error, mock_response]

    soup, prev_url = fetch_list_page(mock_session, PTT_STOCK_INDEX_URL)

    assert soup is not None
    assert prev_url == "https://www.ptt.cc/bbs/stock/index3999.html"
    assert mock_session.get.call_count == 2


def test_fetch_article_detail_retries_on_ssl_error(
    mocker: MockerFixture,
) -> None:
    """測試 fetch_article_detail 遇到 SSL 錯誤自動重試成功。"""
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    import requests as req
    ssl_error = req.exceptions.SSLError("SSL EOF")

    mock_response = mocker.Mock()
    mock_response.text = SAMPLE_ARTICLE_HTML
    mock_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.side_effect = [ssl_error, mock_response]

    result = fetch_article_detail(
        mock_session,
        "https://www.ptt.cc/bbs/stock/M.1740652200.A.B01.html",
    )

    assert result["time_str"] == "14:30:00"
    assert "台積電今日召開法說會" in result["content"]
    assert mock_session.get.call_count == 2


def test_ptt_news_crawler_retries_list_page(
    mocker: MockerFixture,
) -> None:
    """測試完整爬蟲流程中列表頁重試後成功。"""
    mocker.patch("tw_crawler.ptt_news.time.sleep")

    import requests as req
    ssl_error = req.exceptions.SSLError("SSL EOF")

    mock_list_response = mocker.Mock()
    mock_list_response.text = SAMPLE_LIST_HTML
    mock_list_response.raise_for_status = mocker.Mock()

    mock_article_response = mocker.Mock()
    mock_article_response.text = SAMPLE_ARTICLE_HTML
    mock_article_response.raise_for_status = mocker.Mock()

    mock_session = mocker.Mock()
    mock_session.get.side_effect = [
        ssl_error,               # 列表頁第 1 次失敗
        mock_list_response,      # 列表頁第 2 次成功
        mock_article_response,   # 第 1 篇文章
        mock_article_response,   # 第 2 篇文章
    ]
    mock_session.cookies = mocker.Mock()
    mock_session.cookies.get.return_value = "1"
    mock_session.headers = {}

    mocker.patch(
        "tw_crawler.ptt_news._create_session",
        return_value=mock_session,
    )

    df = ptt_news_crawler("2026-02-27")

    assert not df.empty
    assert len(df) == 2
