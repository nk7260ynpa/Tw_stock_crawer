"""CTEE 工商時報股市新聞爬蟲模組。

提供工商時報(CTEE)台股新聞文章爬取與處理功能。
使用 CTEE JSON API 取得新聞列表，cloudscraper 爬取文章全文。
"""

import json
import logging
import re
import time
from datetime import datetime

import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# CTEE 台股新聞列表頁 URL（首頁 HTML，含第 1 頁資料）
CTEE_LIST_URL = "https://www.ctee.com.tw/stock/twmarket"

# CTEE 分頁 JSON API（page >= 2）
CTEE_API_URL = "https://www.ctee.com.tw/api/category/twmarket/{page}"

# 每次 HTTP 請求之間的延遲秒數
REQUEST_DELAY = 1.5

# 最大爬取頁數
MAX_PAGES = 20


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


def _extract_time(text: str) -> str:
    """從日期時間字串中提取時間部分。

    支援 ISO 格式（含 T 分隔符）以及 HH:MM:SS 格式。

    Args:
        text: 可能包含時間的字串。

    Returns:
        時間字串（HH:MM:SS），若無法提取則回傳空字串。
    """
    if not text:
        return ""
    # ISO 格式: "2026-02-26T18:48:12" 或 "2026-02-26T18:48:12+08:00"
    if "T" in text:
        time_part = text.split("T")[1]
        # 移除時區資訊
        for sep in ["+", "Z"]:
            if sep in time_part:
                time_part = time_part.split(sep)[0]
        return time_part
    # 嘗試匹配 HH:MM:SS 或 HH:MM
    match = re.search(r"(\d{1,2}:\d{2}(?::\d{2})?)", text)
    if match:
        return match.group(1)
    return ""


def _parse_date_string(text: str) -> "datetime.date | None":
    """嘗試從文字中解析日期。

    支援的格式：YYYY-MM-DD、YYYY/MM/DD、YYYY.MM.DD、YYYY年MM月DD日。

    Args:
        text: 可能包含日期的文字。

    Returns:
        日期物件，若無法解析則回傳 None。
    """
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
                    return datetime.strptime(match.group(0), fmt).date()
                year, month, day = match.groups()
                return datetime(int(year), int(month), int(day)).date()
            except (ValueError, TypeError):
                continue
    return None


def fetch_list_page_html(
    scraper: cloudscraper.CloudScraper,
) -> str:
    """取得 CTEE 台股新聞列表頁的 HTML（第 1 頁）。

    Args:
        scraper: CloudScraper 實例。

    Returns:
        列表頁的 HTML 字串。
    """
    logger.info("取得 CTEE 列表頁: %s", CTEE_LIST_URL)
    response = scraper.get(CTEE_LIST_URL)
    response.raise_for_status()
    return response.text


def parse_html_list(html: str, target_date: str) -> tuple[list[dict], bool]:
    """從首頁 HTML 解析文章，篩選指定日期。

    Args:
        html: 列表頁 HTML 字串。
        target_date: 目標日期字串（YYYY-MM-DD）。

    Returns:
        tuple: (符合目標日期的文章清單, 是否發現更早日期)。
    """
    soup = BeautifulSoup(html, "lxml")
    articles = []
    found_older = False
    target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()

    for card in soup.select("div.newslist__card"):
        title_tag = card.select_one("h3.news-title a")
        if not title_tag:
            continue

        href = title_tag.get("href", "")
        if not href or "/news/" not in href:
            continue

        url = "https://www.ctee.com.tw" + href if href.startswith("/") else href
        title = title_tag.get_text(strip=True)

        # 從 time.news-time 取得日期
        time_tag = card.select_one("time.news-time")
        article_date = _parse_date_string(
            time_tag.get_text(strip=True)
        ) if time_tag else None

        if article_date:
            if article_date == target_dt:
                articles.append({"url": url, "title": title})
            elif article_date < target_dt:
                found_older = True
        else:
            articles.append({"url": url, "title": title})

    return articles, found_older


def fetch_api_page(
    scraper: cloudscraper.CloudScraper,
    page: int,
) -> list[dict]:
    """透過 CTEE JSON API 取得分頁資料。

    Args:
        scraper: CloudScraper 實例。
        page: 頁碼（>= 2）。

    Returns:
        文章列表（JSON 陣列）。
    """
    url = CTEE_API_URL.format(page=page)
    logger.info("取得 CTEE API 第 %d 頁: %s", page, url)
    response = scraper.get(url)
    response.raise_for_status()
    data = json.loads(response.text)
    return data if isinstance(data, list) else []


def filter_api_articles(
    api_items: list[dict],
    target_date: str,
) -> tuple[list[dict], bool]:
    """從 JSON API 回應中篩選指定日期的文章。

    Args:
        api_items: API 回傳的文章列表。
        target_date: 目標日期字串（YYYY-MM-DD）。

    Returns:
        tuple: (符合日期的文章, 是否發現更早日期)。
    """
    target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    matched = []
    found_older = False

    for item in api_items:
        # publishDatetime 格式: "2026-02-26T18:48:12"
        pub_dt_str = item.get("publishDatetime", "")
        article_date = None
        if pub_dt_str:
            try:
                article_date = datetime.fromisoformat(pub_dt_str).date()
            except (ValueError, TypeError):
                pass

        # 備用: publishDate 格式 "2026.02.26"
        if not article_date:
            article_date = _parse_date_string(
                item.get("publishDate", "")
            )

        if article_date:
            if article_date == target_dt:
                href = item.get("hyperLink", "")
                url = (
                    "https://www.ctee.com.tw" + href
                    if href.startswith("/") else href
                )
                matched.append({
                    "title": item.get("title", ""),
                    "author": item.get("author", ""),
                    "publishDatetime": pub_dt_str,
                    "content_preview": item.get("content", ""),
                    "url": url,
                })
            elif article_date < target_dt:
                found_older = True

    return matched, found_older


def fetch_article_content(
    scraper: cloudscraper.CloudScraper,
    url: str,
) -> dict:
    """爬取文章頁面取得全文與額外資訊。

    Args:
        scraper: CloudScraper 實例。
        url: 文章完整 URL。

    Returns:
        包含 SubHead、HashTag、Content 的字典。
    """
    logger.info("取得 CTEE 文章: %s", url)
    response = scraper.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    # 副標題：og:description
    sub_head = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        sub_head = og_desc.get("content", "")

    # 標籤
    hashtags = []
    meta_tags = soup.find_all("meta", property="article:tag")
    hashtags = [m.get("content", "") for m in meta_tags if m.get("content")]
    if not hashtags:
        for sel in [".post-tags a", ".tags a", "a[rel='tag']"]:
            els = soup.select(sel)
            if els:
                hashtags = [e.get_text(strip=True) for e in els]
                break

    # 全文：div.article-wrap > article > p
    content = ""
    for selector in [
        "div.article-wrap article",
        "div.article-wrap",
        "div.entry-content",
    ]:
        container = soup.select_one(selector)
        if container:
            parts = [
                p.get_text(strip=True) for p in container.find_all("p")
                if p.get_text(strip=True)
                and "剪貼簿" not in p.get_text(strip=True)
            ]
            if parts:
                content = "\n".join(parts).strip()
                break

    return {
        "SubHead": sub_head,
        "HashTag": ",".join(hashtags),
        "Content": content,
    }


def ctee_news_crawler(date: str) -> pd.DataFrame:
    """爬取指定日期的 CTEE 股市新聞。

    流程：
    1. 從首頁 HTML 取得第 1 頁新聞（div.newslist__card）
    2. 透過 JSON API 取得後續分頁（/api/category/twmarket/{page}）
    3. 篩選目標日期的文章
    4. 逐篇爬取文章頁面取得全文
    5. 回傳 DataFrame

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        包含 Date, Time, Author, Head, SubHead, HashTag, url, Content
        欄位的 DataFrame。
    """
    logger.info("開始 CTEE 新聞爬蟲: %s", date)
    scraper = _create_scraper()

    # --- 第 1 頁：從 HTML 取得候選連結 ---
    candidates_from_html = []
    try:
        html = fetch_list_page_html(scraper)
        html_articles, html_found_older = parse_html_list(html, date)
        candidates_from_html = html_articles
        logger.info("首頁 HTML: 找到 %d 篇候選文章", len(html_articles))
    except Exception as e:
        logger.warning("取得首頁 HTML 失敗: %s", e)
        html_found_older = False

    # --- 第 2 頁起：透過 JSON API 取得 ---
    api_articles = []
    stop_paging = html_found_older

    for page_num in range(2, MAX_PAGES + 1):
        if stop_paging:
            break
        try:
            time.sleep(REQUEST_DELAY)
            items = fetch_api_page(scraper, page_num)
            if not items:
                logger.info("API 第 %d 頁無資料，停止翻頁", page_num)
                break

            matched, found_older = filter_api_articles(items, date)
            api_articles.extend(matched)
            logger.info(
                "API 第 %d 頁: %d/%d 篇符合日期",
                page_num, len(matched), len(items),
            )

            if found_older:
                logger.info("發現更早日期文章，停止翻頁")
                stop_paging = True

        except Exception as e:
            logger.warning("取得 API 第 %d 頁失敗: %s", page_num, e)
            break

    # --- 合併候選文章 ---
    # HTML 候選需要逐篇爬取完整資訊；API 候選已有 metadata
    all_urls = set()
    results = []

    # 處理 API 來源的文章（已有 metadata）
    for item in api_articles:
        url = item["url"]
        if url in all_urls:
            continue
        all_urls.add(url)

        try:
            time.sleep(REQUEST_DELAY)
            extra = fetch_article_content(scraper, url)
            results.append({
                "Head": item["title"],
                "SubHead": extra["SubHead"],
                "Author": item["author"],
                "Time": _extract_time(item["publishDatetime"]),
                "HashTag": extra["HashTag"],
                "url": url,
                "Content": extra["Content"],
            })
            logger.info("已解析文章: %s", url)
        except Exception as e:
            logger.warning("爬取文章失敗 %s: %s", url, e)

    # 處理 HTML 候選（僅有 url + title）
    for item in candidates_from_html:
        url = item["url"]
        if url in all_urls:
            continue
        all_urls.add(url)

        try:
            time.sleep(REQUEST_DELAY)
            extra = fetch_article_content(scraper, url)

            # 從文章頁面取得作者
            resp = scraper.get(url)
            soup = BeautifulSoup(resp.text, "lxml")
            author = ""
            meta_author = soup.find("meta", attrs={"name": "author"})
            if meta_author:
                author = meta_author.get("content", "")

            # 時間
            time_str = ""
            time_tag = soup.find("time")
            if time_tag:
                time_str = time_tag.get("datetime", "") or time_tag.get_text(
                    strip=True
                )

            # 驗證日期
            article_date = _parse_date_string(time_str)
            if not article_date:
                try:
                    article_date = datetime.fromisoformat(
                        time_str.replace("Z", "+00:00")
                    ).date()
                except (ValueError, TypeError):
                    pass

            target_dt = datetime.strptime(date, "%Y-%m-%d").date()
            if article_date and article_date != target_dt:
                logger.debug("跳過非目標日期文章: %s", url)
                continue

            results.append({
                "Head": item["title"],
                "SubHead": extra["SubHead"],
                "Author": author,
                "Time": _extract_time(time_str),
                "HashTag": extra["HashTag"],
                "url": url,
                "Content": extra["Content"],
            })
            logger.info("已解析文章: %s", url)
        except Exception as e:
            logger.warning("爬取文章失敗 %s: %s", url, e)

    if not results:
        logger.info("CTEE 新聞 %s 無符合文章", date)
        return _gen_empty_df()

    df = pd.DataFrame(results)
    df["Date"] = date
    # 空字串 Time 轉為 None（MySQL TIME 欄位不接受空字串）
    df["Time"] = df["Time"].replace("", None)

    columns = [
        "Date", "Time", "Author", "Head", "SubHead",
        "HashTag", "url", "Content",
    ]
    df = df[columns]

    logger.info("CTEE 新聞爬蟲完成: %d 篇文章", len(df))
    return df


def _gen_empty_df() -> pd.DataFrame:
    """產生空的 CTEE 新聞 DataFrame。

    Returns:
        具有正確欄位的空 DataFrame。
    """
    return pd.DataFrame(columns=[
        "Date", "Time", "Author", "Head", "SubHead",
        "HashTag", "url", "Content",
    ])
