"""CTEE 工商時報股市新聞爬蟲模組。

提供工商時報(CTEE)台股新聞文章爬取與處理功能。
使用 cloudscraper 繞過 Cloudflare 防護，BeautifulSoup 解析 HTML。
"""

import logging
import time
from datetime import datetime

import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# CTEE 台股新聞列表頁 URL
CTEE_LIST_URL = "https://www.ctee.com.tw/stock/twmarket"

# CTEE 新聞列表頁 URL（含分頁）
CTEE_LIST_URL_PAGED = "https://www.ctee.com.tw/stock/twmarket/page/{page}"

# 每次 HTTP 請求之間的延遲秒數，避免被封鎖
REQUEST_DELAY = 1.5

# 最大爬取頁數（列表頁分頁上限）
MAX_PAGES = 10


def _create_scraper() -> cloudscraper.CloudScraper:
    """建立 cloudscraper 實例。

    Returns:
        設定好的 CloudScraper 實例。
    """
    scraper = cloudscraper.create_scraper(
        browser={
            "browser": "chrome",
            "platform": "windows",
            "mobile": False,
        }
    )
    scraper.headers.update({
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    })
    return scraper


def fetch_list_page(
    scraper: cloudscraper.CloudScraper,
    page: int = 1,
) -> str:
    """取得 CTEE 台股新聞列表頁的 HTML 內容。

    Args:
        scraper: CloudScraper 實例。
        page: 分頁頁碼，預設為 1。

    Returns:
        列表頁的 HTML 字串。

    Raises:
        requests.exceptions.HTTPError: 當 HTTP 狀態碼非 2xx 時拋出。
    """
    if page <= 1:
        url = CTEE_LIST_URL
    else:
        url = CTEE_LIST_URL_PAGED.format(page=page)

    logger.info("Fetching CTEE list page: %s", url)
    response = scraper.get(url)
    response.raise_for_status()
    return response.text


def parse_article_links(html: str, target_date: str) -> list[dict]:
    """從列表頁 HTML 解析文章連結與基本資訊。

    CTEE 列表頁使用 WordPress 架構，文章項目通常位於
    article 或 div 元素中，包含標題連結與日期資訊。

    Args:
        html: 列表頁的 HTML 字串。
        target_date: 目標日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        符合目標日期的文章清單，每個元素為包含 'url' 與 'title' 的字典。
    """
    soup = BeautifulSoup(html, "lxml")
    articles = []
    found_older = False

    # 解析目標日期
    target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()

    # CTEE 使用 WordPress，列表頁文章通常在 article 標籤
    # 或包含 post 相關 class 的容器中
    article_elements = soup.find_all("article")
    if not article_elements:
        # 備選：尋找常見的 WordPress 文章列表結構
        article_elements = soup.find_all(
            "div", class_=lambda c: c and "post" in c
        )
    if not article_elements:
        # 再備選：尋找包含文章連結的列表項目
        article_elements = soup.find_all("li", class_=lambda c: c and "post" in c)

    for article in article_elements:
        # 取得文章連結
        link_tag = article.find("a", href=True)
        if not link_tag:
            continue

        url = link_tag.get("href", "")
        if not url or "/news/" not in url:
            continue

        title = link_tag.get_text(strip=True)

        # 嘗試從文章元素中提取日期
        article_date = _extract_date_from_element(article)

        if article_date:
            if article_date == target_dt:
                articles.append({"url": url, "title": title})
            elif article_date < target_dt:
                found_older = True
        else:
            # 若無法從列表頁取得日期，先收集連結，
            # 後續在文章頁面中確認日期
            articles.append({"url": url, "title": title})

    return articles, found_older


def _extract_date_from_element(element: BeautifulSoup) -> "datetime.date | None":
    """從 HTML 元素中嘗試提取日期。

    依序嘗試以下方式提取日期：
    1. time 標籤的 datetime 屬性
    2. time 標籤的文字內容
    3. 包含日期格式的 span/div 元素

    Args:
        element: BeautifulSoup 元素。

    Returns:
        日期物件，若無法提取則回傳 None。
    """
    # 嘗試 time 標籤
    time_tag = element.find("time")
    if time_tag:
        datetime_attr = time_tag.get("datetime", "")
        if datetime_attr:
            try:
                return datetime.fromisoformat(
                    datetime_attr.replace("Z", "+00:00")
                ).date()
            except (ValueError, TypeError):
                pass
        time_text = time_tag.get_text(strip=True)
        parsed = _parse_date_string(time_text)
        if parsed:
            return parsed

    # 嘗試 post-meta 中的日期文字
    meta_elements = element.find_all(
        ["span", "div"],
        class_=lambda c: c and ("date" in c or "time" in c or "meta" in c),
    )
    for meta in meta_elements:
        text = meta.get_text(strip=True)
        parsed = _parse_date_string(text)
        if parsed:
            return parsed

    return None


def _parse_date_string(text: str) -> "datetime.date | None":
    """嘗試從文字中解析日期。

    支援的格式：
    - 2024-10-15
    - 2024/10/15
    - 2024.10.15
    - 2024年10月15日

    Args:
        text: 可能包含日期的文字。

    Returns:
        日期物件，若無法解析則回傳 None。
    """
    import re

    # 嘗試常見的日期格式
    patterns = [
        (r"(\d{4})-(\d{1,2})-(\d{1,2})", "%Y-%m-%d"),
        (r"(\d{4})/(\d{1,2})/(\d{1,2})", "%Y/%m/%d"),
        (r"(\d{4})\.(\d{1,2})\.(\d{1,2})", "%Y.%m.%d"),
        (r"(\d{4})年(\d{1,2})月(\d{1,2})日", None),
    ]
    for pattern, fmt in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                if fmt:
                    date_str = match.group(0)
                    return datetime.strptime(date_str, fmt).date()
                else:
                    year, month, day = match.groups()
                    return datetime(
                        int(year), int(month), int(day)
                    ).date()
            except (ValueError, TypeError):
                continue

    return None


def fetch_article_page(
    scraper: cloudscraper.CloudScraper,
    url: str,
) -> str:
    """取得 CTEE 文章頁面的 HTML 內容。

    Args:
        scraper: CloudScraper 實例。
        url: 文章 URL。

    Returns:
        文章頁面的 HTML 字串。

    Raises:
        requests.exceptions.HTTPError: 當 HTTP 狀態碼非 2xx 時拋出。
    """
    logger.info("Fetching CTEE article: %s", url)
    response = scraper.get(url)
    response.raise_for_status()
    return response.text


def parse_article(html: str, url: str) -> dict:
    """解析 CTEE 文章頁面 HTML，擷取文章資訊。

    CTEE 使用 WordPress 架構，文章頁面的主要結構：
    - 標題：h1.entry-title 或 h1.post-title
    - 副標題：h2.entry-subtitle 或 .post-subtitle
    - 作者：.author 或 .post-meta 中的連結
    - 時間：time 標籤或 .post-meta 中的日期文字
    - 標籤：.post-tags 或 .entry-tags 中的連結
    - 內文：div.entry-content

    Args:
        html: 文章頁面的 HTML 字串。
        url: 文章 URL（用於記錄）。

    Returns:
        包含以下欄位的字典：
        - Head: 標題
        - SubHead: 副標題
        - Author: 作者
        - Time: 發布時間
        - HashTag: 標籤（逗號分隔）
        - Content: 全文內容
        - url: 文章連結
    """
    soup = BeautifulSoup(html, "lxml")

    head = _extract_title(soup)
    sub_head = _extract_subtitle(soup)
    author = _extract_author(soup)
    pub_time = _extract_time(soup)
    hashtag = _extract_hashtags(soup)
    content = _extract_content(soup)

    return {
        "Head": head,
        "SubHead": sub_head,
        "Author": author,
        "Time": pub_time,
        "HashTag": hashtag,
        "Content": content,
        "url": url,
    }


def _extract_title(soup: BeautifulSoup) -> str:
    """從文章頁面提取標題。

    Args:
        soup: BeautifulSoup 物件。

    Returns:
        文章標題字串。
    """
    # 嘗試常見的 WordPress 標題 class
    for selector in [
        "h1.entry-title",
        "h1.post-title",
        "h1.article-title",
        ".entry-header h1",
        ".post-header h1",
    ]:
        tag = soup.select_one(selector)
        if tag:
            return tag.get_text(strip=True)

    # 備選：直接找 h1
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True)

    # 再備選：從 og:title meta 標籤取得
    og_title = soup.find("meta", property="og:title")
    if og_title:
        return og_title.get("content", "")

    return ""


def _extract_subtitle(soup: BeautifulSoup) -> str:
    """從文章頁面提取副標題。

    Args:
        soup: BeautifulSoup 物件。

    Returns:
        副標題字串，若無則回傳空字串。
    """
    for selector in [
        ".entry-subtitle",
        ".post-subtitle",
        "h2.subtitle",
        ".article-subtitle",
        ".subheadline",
    ]:
        tag = soup.select_one(selector)
        if tag:
            return tag.get_text(strip=True)

    # 嘗試從 og:description 取得
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        return og_desc.get("content", "")

    return ""


def _extract_author(soup: BeautifulSoup) -> str:
    """從文章頁面提取作者。

    Args:
        soup: BeautifulSoup 物件。

    Returns:
        作者名稱字串。
    """
    # 嘗試 meta 標籤
    meta_author = soup.find("meta", attrs={"name": "author"})
    if meta_author:
        return meta_author.get("content", "")

    # 嘗試常見的作者 class
    for selector in [
        ".author a",
        ".author-name",
        ".post-author a",
        ".entry-author a",
        "a[rel='author']",
        ".byline a",
    ]:
        tag = soup.select_one(selector)
        if tag:
            return tag.get_text(strip=True)

    # 嘗試 article:author meta
    og_author = soup.find("meta", property="article:author")
    if og_author:
        return og_author.get("content", "")

    return ""


def _extract_time(soup: BeautifulSoup) -> str:
    """從文章頁面提取發布時間。

    Args:
        soup: BeautifulSoup 物件。

    Returns:
        發布時間字串。
    """
    # 嘗試 time 標籤
    time_tag = soup.find("time")
    if time_tag:
        datetime_attr = time_tag.get("datetime", "")
        if datetime_attr:
            return datetime_attr
        return time_tag.get_text(strip=True)

    # 嘗試 meta 標籤
    for prop in [
        "article:published_time",
        "article:modified_time",
        "datePublished",
    ]:
        meta = soup.find("meta", property=prop)
        if meta:
            return meta.get("content", "")

    # 嘗試 post-meta 中的日期文字
    for selector in [".post-meta", ".entry-meta", ".article-meta", ".date"]:
        tag = soup.select_one(selector)
        if tag:
            return tag.get_text(strip=True)

    return ""


def _extract_hashtags(soup: BeautifulSoup) -> str:
    """從文章頁面提取標籤。

    Args:
        soup: BeautifulSoup 物件。

    Returns:
        逗號分隔的標籤字串。
    """
    tags = []

    # 嘗試常見的標籤容器
    for selector in [
        ".post-tags a",
        ".entry-tags a",
        ".tags a",
        ".tag-links a",
        ".article-tags a",
        "a[rel='tag']",
    ]:
        elements = soup.select(selector)
        if elements:
            tags = [el.get_text(strip=True) for el in elements]
            break

    # 嘗試 article:tag meta
    if not tags:
        meta_tags = soup.find_all("meta", property="article:tag")
        tags = [m.get("content", "") for m in meta_tags if m.get("content")]

    return ",".join(tags)


def _extract_content(soup: BeautifulSoup) -> str:
    """從文章頁面提取全文內容。

    CTEE 文章內文位於 div.entry-content 容器中，
    由多個 <p> 標籤組成。

    Args:
        soup: BeautifulSoup 物件。

    Returns:
        全文內容字串。
    """
    # 嘗試常見的 WordPress 內文容器
    for selector in [
        "div.entry-content.clearfix.single-post-content",
        "div.entry-content",
        "div.post-content",
        "div.article-content",
        "div.content-inner",
    ]:
        container = soup.select_one(selector)
        if container:
            paragraphs = container.find_all("p")
            if paragraphs:
                text = "\n".join(
                    p.get_text(strip=True) for p in paragraphs
                    if p.get_text(strip=True)
                )
                return text.strip()

    return ""


def _get_article_date(article_data: dict) -> "datetime.date | None":
    """從文章資料中提取日期。

    Args:
        article_data: parse_article 回傳的字典。

    Returns:
        日期物件，若無法提取則回傳 None。
    """
    time_str = article_data.get("Time", "")
    if not time_str:
        return None

    parsed = _parse_date_string(time_str)
    if parsed:
        return parsed

    # 嘗試 ISO 格式（datetime 屬性常用的格式）
    try:
        return datetime.fromisoformat(
            time_str.replace("Z", "+00:00")
        ).date()
    except (ValueError, TypeError):
        pass

    return None


def ctee_news_crawler(date: str) -> pd.DataFrame:
    """爬取指定日期的 CTEE 股市新聞。

    流程：
    1. 用 cloudscraper 存取 CTEE 台股新聞列表頁
    2. 解析新聞列表頁的所有文章連結，篩選指定日期的文章
    3. 逐篇存取文章頁面，擷取完整資訊
    4. 回傳 DataFrame

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        包含 Date, Time, Author, Head, SubHead, HashTag, url, Content
        欄位的 DataFrame。若無資料則回傳空的 DataFrame。
    """
    logger.info("Starting CTEE news crawler for date: %s", date)
    scraper = _create_scraper()
    target_dt = datetime.strptime(date, "%Y-%m-%d").date()

    # 收集候選文章連結
    candidate_links = []

    for page_num in range(1, MAX_PAGES + 1):
        try:
            html = fetch_list_page(scraper, page_num)
            articles, found_older = parse_article_links(html, date)
            candidate_links.extend(articles)

            logger.info(
                "Page %d: found %d candidate articles",
                page_num, len(articles),
            )

            # 如果發現比目標日期更早的文章，停止翻頁
            if found_older:
                logger.info(
                    "Found articles older than %s, stopping pagination",
                    date,
                )
                break

            time.sleep(REQUEST_DELAY)

        except Exception as e:
            logger.warning(
                "Failed to fetch list page %d: %s", page_num, e
            )
            break

    if not candidate_links:
        logger.info("No candidate articles found for date: %s", date)
        return _gen_empty_df()

    # 去除重複的 URL
    seen_urls = set()
    unique_links = []
    for link in candidate_links:
        if link["url"] not in seen_urls:
            seen_urls.add(link["url"])
            unique_links.append(link)

    logger.info(
        "Total unique candidate articles: %d", len(unique_links)
    )

    # 逐篇爬取文章內容
    results = []
    for link in unique_links:
        try:
            time.sleep(REQUEST_DELAY)
            article_html = fetch_article_page(scraper, link["url"])
            article_data = parse_article(article_html, link["url"])

            # 確認文章日期是否符合目標日期
            article_date = _get_article_date(article_data)
            if article_date and article_date != target_dt:
                logger.debug(
                    "Skipping article (date mismatch): %s (got %s, want %s)",
                    link["url"], article_date, date,
                )
                continue

            results.append(article_data)
            logger.info("Successfully parsed article: %s", link["url"])

        except Exception as e:
            logger.warning(
                "Failed to fetch/parse article %s: %s", link["url"], e
            )
            continue

    if not results:
        logger.info("No articles matched date: %s", date)
        return _gen_empty_df()

    df = pd.DataFrame(results)
    df["Date"] = date
    df["Date"] = pd.to_datetime(df["Date"])

    # 重新排列欄位順序
    columns = [
        "Date", "Time", "Author", "Head", "SubHead",
        "HashTag", "url", "Content",
    ]
    df = df[columns]

    logger.info(
        "CTEE news crawler completed, articles: %d", len(df)
    )
    return df


def _gen_empty_df() -> pd.DataFrame:
    """產生 CTEE 新聞爬蟲無資料時的空 DataFrame。

    Returns:
        具有正確欄位的空 DataFrame。
    """
    return pd.DataFrame(columns=[
        "Date", "Time", "Author", "Head", "SubHead",
        "HashTag", "url", "Content",
    ])
