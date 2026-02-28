"""聯合新聞網經濟日報台股新聞爬蟲模組。

提供 MoneyUDN（聯合新聞網-經濟日報）台股新聞文章爬取與處理功能。
使用 requests + BeautifulSoup 解析列表頁 JSON-LD 結構化資料，
markdownify 將文章內文（含圖片）轉為 Markdown。

支援兩種模式：
- 日期模式（date）：抓取指定日期的全部新聞
- 時數模式（hours）：抓取過去 N 小時內的新聞，避免排程間隔漏抓
"""

import json
import logging
import random
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md

# 台灣時區 (UTC+8)
TW_TZ = timezone(timedelta(hours=8))

logger = logging.getLogger(__name__)

# MoneyUDN 經濟日報最新文章列表 URL（支援分頁，{page} 由程式替換）
MONEYUDN_LIST_URL_TEMPLATE = (
    "https://money.udn.com/rank/newest/1001/5591/{page}"
)

# MoneyUDN 基礎 URL
MONEYUDN_BASE_URL = "https://money.udn.com"

# HTTP 請求逾時秒數
REQUEST_TIMEOUT = 30

# 文章間延遲範圍（秒）
REQUEST_DELAY_MIN = 1.0
REQUEST_DELAY_MAX = 2.0

# 列表頁最大翻頁數（避免無限爬取）
MAX_LIST_PAGES = 10

# HTTP 請求 headers
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://money.udn.com/",
}


def _create_session() -> requests.Session:
    """建立已設定 headers 的 requests session。

    Returns:
        設定好 User-Agent 與 Referer 等 headers 的 Session 實例。
    """
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def _parse_list_page(html: str) -> list[dict]:
    """解析列表頁 HTML 中的 JSON-LD 結構化資料。

    從列表頁的 <script type="application/ld+json"> 標籤中提取
    ItemList 內的 NewsArticle 清單。

    Args:
        html: 列表頁完整 HTML 字串。

    Returns:
        文章資訊清單，每筆包含 name, url, datePublished, author 等欄位。
        若解析失敗則回傳空清單。
    """
    soup = BeautifulSoup(html, "lxml")
    ld_scripts = soup.find_all("script", type="application/ld+json")

    for script in ld_scripts:
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue

        # JSON-LD 可能是 @graph 陣列或單一物件
        graph = data.get("@graph", [data])

        for item in graph:
            if item.get("@type") == "ItemList":
                elements = item.get("itemListElement", [])
                articles = []
                for element in elements:
                    article = element.get("item", element)
                    if article.get("@type") == "NewsArticle":
                        articles.append(article)
                if articles:
                    return articles

    logger.warning("列表頁中找不到 JSON-LD ItemList 資料")
    return []


def _build_full_url(relative_url: str) -> str:
    """將相對 URL 轉為完整 URL。

    Args:
        relative_url: 相對或完整 URL。

    Returns:
        完整的 URL 字串（去除查詢參數中的 from 追蹤參數）。
    """
    # 去除追蹤參數
    url = relative_url.split("?")[0]

    if url.startswith("http"):
        return url
    return MONEYUDN_BASE_URL + url


def _parse_published_date(date_str: str) -> datetime | None:
    """解析 JSON-LD 的 datePublished 欄位。

    支援 ISO 8601 格式，例如 '2026-02-27T20:30:31+08:00'
    或簡化格式 '2026-02-27 20:30:31'。

    Args:
        date_str: datePublished 日期字串。

    Returns:
        datetime 物件，若無法解析則回傳 None。
    """
    if not date_str:
        return None

    # 嘗試多種格式
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",      # ISO 8601 含時區
        "%Y-%m-%dT%H:%M:%S",         # ISO 8601 無時區
        "%Y-%m-%d %H:%M:%S",         # 簡化格式
        "%Y-%m-%d %H:%M",            # 簡化格式無秒
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    logger.warning("無法解析日期: %s", date_str)
    return None


def _extract_author(article: dict) -> str:
    """從 JSON-LD 文章物件中提取作者名稱。

    Args:
        article: JSON-LD NewsArticle 字典。

    Returns:
        作者名稱字串，若無法取得則回傳空字串。
    """
    author = article.get("author")
    if not author:
        return ""

    if isinstance(author, dict):
        return author.get("name", "")
    if isinstance(author, list):
        names = [
            a.get("name", "") if isinstance(a, dict) else str(a)
            for a in author
        ]
        return ",".join(n for n in names if n)
    return str(author)


def _extract_hero_image(soup: BeautifulSoup) -> str:
    """提取文章主圖（hero image）為 Markdown 格式。

    UDN 文章的主圖位於 article_body 外部的 <figure class="article-image">
    標籤中，需要額外提取。

    Args:
        soup: 文章頁面的 BeautifulSoup 物件。

    Returns:
        主圖的 Markdown 字串（如 ``![caption](url)``），若無主圖則回傳空字串。
    """
    figure = soup.find("figure", class_="article-image")
    if not figure:
        return ""

    img = figure.find("img")
    if not img:
        return ""

    src = img.get("src", "") or img.get("data-src", "")
    if not src:
        return ""

    # 取得圖說（figcaption）
    caption = ""
    figcaption = figure.find("figcaption")
    if figcaption:
        caption = figcaption.get_text(strip=True)

    return f"![{caption}]({src})"


def _fetch_article_content(session: requests.Session, url: str) -> str:
    """爬取文章內頁並提取全文為含圖片的 Markdown。

    從文章頁面的 <figure class="article-image"> 提取主圖，
    從 section#article_body（class=article-body__editor）提取內文，
    使用 markdownify 轉換，保留圖片為 ![alt](src) 格式。

    Args:
        session: 已設定 headers 的 requests Session。
        url: 文章完整 URL。

    Returns:
        Markdown 格式的文章全文（含主圖），若無法提取則回傳空字串。

    Raises:
        requests.RequestException: 當 HTTP 請求失敗時。
    """
    logger.info("取得 MoneyUDN 文章: %s", url)
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    # 提取主圖（位於 article_body 外部的 figure.article-image）
    hero_image = _extract_hero_image(soup)

    # 尋找文章內容容器（UDN 使用 section 而非 div）
    article_body = soup.find(id="article_body")
    if not article_body:
        article_body = soup.find("section", class_="article-body__editor")
    if not article_body:
        logger.warning("文章頁面找不到 article-body: %s", url)
        return hero_image if hero_image else ""

    # 移除廣告區塊和不必要的元素
    for ad in article_body.find_all("div", class_="edn-ads--inlineAds"):
        ad.decompose()
    for tag in article_body.find_all(["script", "style"]):
        tag.decompose()

    # 使用 markdownify 轉換，保留圖片但移除 script 和 style
    body_content = md(
        str(article_body),
        strip=["script", "style"],
    ).strip()

    # 主圖放在內文最前面
    if hero_image:
        return f"{hero_image}\n\n{body_content}"

    return body_content


def _collect_candidates_from_pages(
    session: requests.Session,
    target_date: "date",
    cutoff_dt: datetime | None = None,
) -> list[dict]:
    """從多頁列表中收集符合條件的所有文章候選。

    支援兩種模式：
    - 日期模式（cutoff_dt=None）：篩選目標日期的文章
    - 時數模式（cutoff_dt 有值）：篩選 cutoff_dt 之後的文章

    逐頁爬取 /rank/newest/ 列表，當發現超出範圍的文章時停止翻頁。

    Args:
        session: 已設定 headers 的 requests Session。
        target_date: 目標日期物件（日期模式使用）。
        cutoff_dt: 時數模式的截止時間點，篩選此時間之後的文章。

    Returns:
        符合條件的文章候選清單。
    """
    candidates = []
    seen_urls = set()

    for page in range(1, MAX_LIST_PAGES + 1):
        url = MONEYUDN_LIST_URL_TEMPLATE.format(page=page)
        logger.info("取得 MoneyUDN 列表頁 %d: %s", page, url)

        try:
            if page > 1:
                time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error("取得 MoneyUDN 列表頁 %d 失敗: %s", page, e)
            break

        articles = _parse_list_page(response.text)
        if not articles:
            logger.info("MoneyUDN 列表頁 %d 無文章資料，停止翻頁", page)
            break

        passed_target = False
        for article in articles:
            date_published = article.get("datePublished", "")
            published_dt = _parse_published_date(date_published)

            if published_dt is None:
                continue

            if cutoff_dt is not None:
                # 時數模式：比對完整時間
                # 需處理 aware vs naive datetime
                pub_naive = (
                    published_dt.replace(tzinfo=None)
                    if published_dt.tzinfo
                    else published_dt
                )
                cutoff_naive = cutoff_dt.replace(tzinfo=None)

                if pub_naive < cutoff_naive:
                    passed_target = True
                    break
            else:
                # 日期模式
                if published_dt.date() < target_date:
                    passed_target = True
                    break

                if published_dt.date() != target_date:
                    continue

            url_raw = article.get("url", "")
            if not url_raw:
                continue

            full_url = _build_full_url(url_raw)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            candidates.append({
                "name": (
                    article.get("name", "") or article.get("headline", "")
                ),
                "url": full_url,
                "author": _extract_author(article),
                "time_str": published_dt.strftime("%H:%M:%S"),
                "date_str": published_dt.strftime("%Y-%m-%d"),
            })

        if passed_target:
            logger.info("列表頁 %d 已超過範圍，停止翻頁", page)
            break

    return candidates


def moneyudn_news_crawler(
    date: str,
    hours: int | None = None,
) -> pd.DataFrame:
    """爬取聯合新聞網經濟日報台股新聞。

    支援兩種模式：
    - 日期模式（hours=None）：抓取指定日期的全部新聞
    - 時數模式（hours 有值）：抓取過去 N 小時內的新聞

    流程：
    1. 逐頁爬取最新文章列表，解析 JSON-LD 取得文章清單
    2. 依模式篩選文章（日期或時間區間）
    3. 逐篇爬取文章內頁，用 markdownify 轉為含圖片的 Markdown
    4. 文章間加入隨機延遲以避免被封鎖

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。
        hours: 抓取過去幾小時的新聞。若為 None 則使用日期模式。

    Returns:
        包含 Date, Time, Author, Head, url, Content
        欄位的 DataFrame。若無符合條件的文章則回傳空 DataFrame。
    """
    # 計算時間截止點（時數模式）
    cutoff_dt = None
    if hours is not None:
        now = datetime.now(tz=TW_TZ)
        cutoff_dt = now - timedelta(hours=hours)
        logger.info(
            "開始 MoneyUDN 新聞爬蟲（時數模式）: 過去 %d 小時 "
            "（截止時間: %s）",
            hours, cutoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
        )
    else:
        logger.info("開始 MoneyUDN 新聞爬蟲（日期模式）: %s", date)

    target_date = datetime.strptime(date, "%Y-%m-%d").date()
    session = _create_session()

    # 步驟 1+2：逐頁取得列表並篩選文章
    candidates = _collect_candidates_from_pages(
        session, target_date, cutoff_dt=cutoff_dt,
    )

    if not candidates:
        logger.info("MoneyUDN 無符合條件的文章")
        return _gen_empty_df()

    logger.info("MoneyUDN 共 %d 篇符合條件", len(candidates))

    # 步驟 3：逐篇爬取文章全文
    results = []
    for i, candidate in enumerate(candidates):
        url = candidate["url"]

        try:
            if i > 0:
                delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
                time.sleep(delay)

            content = _fetch_article_content(session, url)

            # 使用文章自身的日期（時數模式下可能跨日）
            article_date = candidate.get("date_str", date)

            results.append({
                "Date": article_date,
                "Time": candidate["time_str"],
                "Author": candidate["author"],
                "Head": candidate["name"],
                "url": url,
                "Content": content,
            })
            logger.info("已解析文章: %s", url)
        except Exception as e:
            logger.warning("爬取文章失敗 %s: %s", url, e)

    if not results:
        logger.info("MoneyUDN 無符合條件的文章（全文解析後）")
        return _gen_empty_df()

    df = pd.DataFrame(results)
    # 空字串 Time 轉為 None（MySQL TIME 欄位不接受空字串）
    df["Time"] = df["Time"].replace("", None)

    columns = ["Date", "Time", "Author", "Head", "url", "Content"]
    df = df[columns]

    logger.info("MoneyUDN 新聞爬蟲完成: %d 篇文章", len(df))
    return df


def _gen_empty_df() -> pd.DataFrame:
    """產生空的 MoneyUDN 新聞 DataFrame。

    Returns:
        具有正確欄位的空 DataFrame。
    """
    return pd.DataFrame(
        columns=["Date", "Time", "Author", "Head", "url", "Content"]
    )
