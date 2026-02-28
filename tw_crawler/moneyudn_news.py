"""聯合新聞網經濟日報台股新聞爬蟲模組。

提供 MoneyUDN（聯合新聞網-經濟日報）台股新聞文章爬取與處理功能。
使用 requests + BeautifulSoup 解析列表頁 JSON-LD 結構化資料，
markdownify 將文章內文（含圖片）轉為 Markdown。
"""

import json
import logging
import random
import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md

logger = logging.getLogger(__name__)

# MoneyUDN 經濟日報熱門排行榜 URL
MONEYUDN_LIST_URL = "https://money.udn.com/rank/pv/1001/5591/1"

# MoneyUDN 基礎 URL
MONEYUDN_BASE_URL = "https://money.udn.com"

# HTTP 請求逾時秒數
REQUEST_TIMEOUT = 30

# 文章間延遲範圍（秒）
REQUEST_DELAY_MIN = 1.0
REQUEST_DELAY_MAX = 2.0

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


def _fetch_article_content(session: requests.Session, url: str) -> str:
    """爬取文章內頁並提取全文為含圖片的 Markdown。

    從文章頁面的 section#article_body（class=article-body__editor）提取內文，
    使用 markdownify 轉換，保留圖片為 ![alt](src) 格式。

    Args:
        session: 已設定 headers 的 requests Session。
        url: 文章完整 URL。

    Returns:
        Markdown 格式的文章全文，若無法提取則回傳空字串。

    Raises:
        requests.RequestException: 當 HTTP 請求失敗時。
    """
    logger.info("取得 MoneyUDN 文章: %s", url)
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    # 尋找文章內容容器（UDN 使用 section 而非 div）
    article_body = soup.find(id="article_body")
    if not article_body:
        article_body = soup.find("section", class_="article-body__editor")
    if not article_body:
        logger.warning("文章頁面找不到 article-body: %s", url)
        return ""

    # 移除廣告區塊和不必要的元素
    for ad in article_body.find_all("div", class_="edn-ads--inlineAds"):
        ad.decompose()
    for tag in article_body.find_all(["script", "style"]):
        tag.decompose()

    # 使用 markdownify 轉換，保留圖片但移除 script 和 style
    content = md(
        str(article_body),
        strip=["script", "style"],
    ).strip()

    return content


def moneyudn_news_crawler(date: str) -> pd.DataFrame:
    """爬取指定日期的聯合新聞網經濟日報台股新聞。

    流程：
    1. 爬取列表頁，解析 JSON-LD 取得文章列表
    2. 根據 datePublished 篩選目標日期的文章
    3. 逐篇爬取文章內頁，用 markdownify 轉為含圖片的 Markdown
    4. 文章間加入隨機延遲以避免被封鎖

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        包含 Date, Time, Author, Head, url, Content
        欄位的 DataFrame。若無該日期文章則回傳空 DataFrame。
    """
    logger.info("開始 MoneyUDN 新聞爬蟲: %s", date)
    target_date = datetime.strptime(date, "%Y-%m-%d").date()
    session = _create_session()

    # 步驟 1：取得列表頁
    try:
        logger.info("取得 MoneyUDN 列表頁: %s", MONEYUDN_LIST_URL)
        response = session.get(MONEYUDN_LIST_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("取得 MoneyUDN 列表頁失敗: %s", e)
        return _gen_empty_df()

    # 步驟 2：解析 JSON-LD 取得文章列表
    articles = _parse_list_page(response.text)
    if not articles:
        logger.info("MoneyUDN 列表頁無文章資料")
        return _gen_empty_df()

    logger.info("MoneyUDN 列表頁共 %d 篇文章", len(articles))

    # 步驟 3：篩選目標日期的文章
    candidates = []
    for article in articles:
        date_published = article.get("datePublished", "")
        published_dt = _parse_published_date(date_published)

        if published_dt is None:
            logger.warning(
                "文章缺少有效的 datePublished: %s",
                article.get("name", "unknown"),
            )
            continue

        if published_dt.date() != target_date:
            continue

        url_raw = article.get("url", "")
        if not url_raw:
            continue

        candidates.append({
            "name": article.get("name", "") or article.get("headline", ""),
            "url": _build_full_url(url_raw),
            "author": _extract_author(article),
            "time_str": published_dt.strftime("%H:%M:%S"),
        })

    if not candidates:
        logger.info("MoneyUDN %s 無符合日期的文章", date)
        return _gen_empty_df()

    logger.info("MoneyUDN %s 共 %d 篇符合日期", date, len(candidates))

    # 步驟 4：逐篇爬取文章全文
    results = []
    for i, candidate in enumerate(candidates):
        url = candidate["url"]

        try:
            if i > 0:
                delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
                time.sleep(delay)

            content = _fetch_article_content(session, url)

            results.append({
                "Date": date,
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
        logger.info("MoneyUDN %s 無符合文章（全文解析後）", date)
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
