"""PTT 股版新聞爬蟲模組。

提供 PTT Stock 版（批踢踢股票版）文章爬取與處理功能。
使用 requests + BeautifulSoup 爬取列表頁與文章頁，
markdownify 將內文轉為 Markdown。
"""

import logging
import re
import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md

logger = logging.getLogger(__name__)

# PTT Stock 版首頁 URL
PTT_BASE_URL = "https://www.ptt.cc"
PTT_STOCK_INDEX_URL = f"{PTT_BASE_URL}/bbs/stock/index.html"

# 每次 HTTP 請求之間的延遲秒數
REQUEST_DELAY = 0.5

# HTTP 請求逾時秒數
REQUEST_TIMEOUT = 30


def _create_session() -> requests.Session:
    """建立已設定 PTT 年齡驗證 cookie 的 requests session。

    Returns:
        設定好 over18 cookie 的 Session 實例。
    """
    session = requests.Session()
    session.cookies.set("over18", "1")
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    })
    return session


def _parse_list_date(date_text: str, target_year: int) -> datetime | None:
    """將 PTT 列表頁的月/日格式轉換為完整日期。

    PTT 列表頁顯示的日期格式為 'M/DD' 或 ' M/DD'（例如 '2/27' 或 '12/31'），
    不包含年份，需根據目標年份推算。

    Args:
        date_text: PTT 列表頁的日期文字（如 '2/27'）。
        target_year: 目標查詢年份。

    Returns:
        完整的 datetime 物件，若無法解析則回傳 None。
    """
    date_text = date_text.strip()
    if not date_text:
        return None

    match = re.match(r"(\d{1,2})/(\d{1,2})", date_text)
    if not match:
        return None

    month = int(match.group(1))
    day = int(match.group(2))

    try:
        return datetime(target_year, month, day)
    except ValueError:
        return None


def _parse_article_time(meta_time_str: str) -> datetime | None:
    """解析 PTT 文章頁面的完整時間戳。

    PTT 文章頁面中的時間格式為 'Fri Feb 27 14:30:00 2026'。

    Args:
        meta_time_str: PTT 文章內 metaline 的時間字串。

    Returns:
        datetime 物件，若無法解析則回傳 None。
    """
    meta_time_str = meta_time_str.strip()
    if not meta_time_str:
        return None

    try:
        return datetime.strptime(meta_time_str, "%a %b %d %H:%M:%S %Y")
    except ValueError:
        logger.warning("無法解析文章時間: %s", meta_time_str)
        return None


def _extract_article_content(soup: BeautifulSoup) -> str:
    """從文章頁面提取全文並轉為 Markdown。

    去除 metaline（文章標頭）和推文（div.push）後，
    將剩餘的 main-content 內容用 markdownify 轉為 Markdown。

    Args:
        soup: 文章頁面的 BeautifulSoup 物件。

    Returns:
        Markdown 格式的文章全文，若無法提取則回傳空字串。
    """
    main_content = soup.find("div", id="main-content")
    if not main_content:
        return ""

    # 複製節點以避免修改原始 soup
    content_copy = BeautifulSoup(str(main_content), "lxml")
    main_div = content_copy.find("div", id="main-content")
    if not main_div:
        return ""

    # 移除 metaline（文章標頭資訊）
    for meta in main_div.find_all("div", class_="article-metaline"):
        meta.decompose()
    for meta in main_div.find_all("div", class_="article-metaline-right"):
        meta.decompose()

    # 移除推文區塊
    for push in main_div.find_all("div", class_="push"):
        push.decompose()

    # 移除 "※ 發信站" 以後的簽名檔
    text = main_div.get_text()
    sig_markers = ["※ 發信站", "--"]
    for marker in sig_markers:
        idx = text.rfind(marker)
        if idx != -1:
            text = text[:idx]
            break

    # 直接使用清理後的文字，避免 markdownify 對純文字產生不必要的轉換
    # 因為 PTT 文章本身就是純文字格式
    content = text.strip()

    # 若清理後內容過短（可能清理過度），改用 markdownify 處理完整 HTML
    if len(content) < 10:
        content = md(str(main_div), strip=["img", "script", "style"]).strip()

    return content


def fetch_list_page(
    session: requests.Session,
    url: str,
) -> tuple[BeautifulSoup, str | None]:
    """取得 PTT 列表頁面並解析。

    Args:
        session: 已設定 cookie 的 requests Session。
        url: 列表頁 URL。

    Returns:
        tuple: (BeautifulSoup 物件, 上一頁的 URL 或 None)。

    Raises:
        requests.RequestException: 當 HTTP 請求失敗時。
    """
    logger.info("取得 PTT 列表頁: %s", url)
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    # 取得「上頁」連結
    prev_url = None
    paging = soup.find("div", class_="btn-group-paging")
    if paging:
        links = paging.find_all("a")
        for link in links:
            if "上頁" in link.get_text():
                href = link.get("href", "")
                if href:
                    prev_url = PTT_BASE_URL + href
                break

    return soup, prev_url


def parse_list_articles(
    soup: BeautifulSoup,
    target_date: str,
    target_year: int,
) -> tuple[list[dict], bool]:
    """從列表頁解析文章資訊，篩選指定日期。

    Args:
        soup: 列表頁的 BeautifulSoup 物件。
        target_date: 目標日期字串（YYYY-MM-DD）。
        target_year: 目標查詢年份（用於補足 PTT 列表日期的年份）。

    Returns:
        tuple: (符合目標日期的文章清單, 是否發現比目標日期更早的文章)。
            每篇文章包含 url, title, author 欄位。
    """
    target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    articles = []
    found_older = False

    entries = soup.find_all("div", class_="r-ent")
    for entry in entries:
        # 取得標題和連結
        title_div = entry.find("div", class_="title")
        if not title_div:
            continue

        title_link = title_div.find("a")
        if not title_link:
            # 已刪除的文章沒有 <a> 標籤，跳過
            continue

        href = title_link.get("href", "")
        title = title_link.get_text(strip=True)
        url = PTT_BASE_URL + href if href else ""

        # 取得作者
        meta_div = entry.find("div", class_="meta")
        author = ""
        if meta_div:
            author_div = meta_div.find("div", class_="author")
            if author_div:
                author = author_div.get_text(strip=True)

        # 取得日期
        date_div = None
        if meta_div:
            date_div = meta_div.find("div", class_="date")

        if date_div:
            date_text = date_div.get_text(strip=True)
            article_dt = _parse_list_date(date_text, target_year)

            if article_dt:
                article_date = article_dt.date()
                if article_date == target_dt:
                    articles.append({
                        "url": url,
                        "title": title,
                        "author": author,
                    })
                elif article_date < target_dt:
                    found_older = True
            else:
                # 無法解析日期，保守地納入候選
                articles.append({
                    "url": url,
                    "title": title,
                    "author": author,
                })
        else:
            # 無日期資訊，保守地納入候選
            articles.append({
                "url": url,
                "title": title,
                "author": author,
            })

    return articles, found_older


def fetch_article_detail(
    session: requests.Session,
    url: str,
) -> dict:
    """爬取 PTT 文章頁面取得完整時間與全文。

    Args:
        session: 已設定 cookie 的 requests Session。
        url: 文章完整 URL。

    Returns:
        包含 time_str（HH:MM:SS）、full_datetime（datetime）、
        content（Markdown 全文）的字典。

    Raises:
        requests.RequestException: 當 HTTP 請求失敗時。
    """
    logger.info("取得 PTT 文章: %s", url)
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    # 解析完整時間：第三個 span.article-meta-value
    full_dt = None
    time_str = ""
    meta_values = soup.select("div.article-metaline span.article-meta-value")
    if len(meta_values) >= 3:
        time_text = meta_values[2].get_text(strip=True)
        full_dt = _parse_article_time(time_text)
        if full_dt:
            time_str = full_dt.strftime("%H:%M:%S")

    # 提取全文
    content = _extract_article_content(soup)

    return {
        "time_str": time_str,
        "full_datetime": full_dt,
        "content": content,
    }


def ptt_news_crawler(date: str) -> pd.DataFrame:
    """爬取指定日期的 PTT 股版文章。

    流程：
    1. 從 PTT Stock 版最新頁開始
    2. 解析列表頁文章，篩選目標日期
    3. 逐篇爬取文章頁面取得完整時間與全文
    4. 使用 markdownify 將內文轉為 Markdown
    5. 遇到比目標日期更早的文章時停止翻頁

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。

    Returns:
        包含 Date, Time, Author, Head, url, Content
        欄位的 DataFrame。若無該日期文章則回傳空 DataFrame。
    """
    logger.info("開始 PTT 股版爬蟲: %s", date)
    target_year = datetime.strptime(date, "%Y-%m-%d").year
    session = _create_session()

    all_candidates = []
    current_url = PTT_STOCK_INDEX_URL

    while current_url:
        try:
            soup, prev_url = fetch_list_page(session, current_url)
        except requests.RequestException as e:
            logger.error("取得 PTT 列表頁失敗: %s", e)
            break

        matched, found_older = parse_list_articles(
            soup, date, target_year,
        )
        all_candidates.extend(matched)
        logger.info(
            "PTT 列表頁 %s: %d 篇符合日期",
            current_url.split("/")[-1], len(matched),
        )

        if found_older:
            logger.info("發現更早日期文章，停止翻頁")
            break

        current_url = prev_url
        if current_url:
            time.sleep(REQUEST_DELAY)

    if not all_candidates:
        logger.info("PTT 股版 %s 無符合文章", date)
        return _gen_empty_df()

    # 逐篇爬取文章全文
    target_dt = datetime.strptime(date, "%Y-%m-%d").date()
    results = []
    for candidate in all_candidates:
        url = candidate["url"]
        if not url:
            continue

        try:
            time.sleep(REQUEST_DELAY)
            detail = fetch_article_detail(session, url)

            # 用文章頁面的完整時間再次驗證日期
            full_dt = detail.get("full_datetime")
            if full_dt and full_dt.date() != target_dt:
                logger.debug(
                    "文章實際日期 %s 與目標 %s 不符，跳過: %s",
                    full_dt.date(), target_dt, url,
                )
                continue

            results.append({
                "Date": date,
                "Time": detail["time_str"],
                "Author": candidate["author"],
                "Head": candidate["title"],
                "url": url,
                "Content": detail["content"],
            })
            logger.info("已解析文章: %s", url)
        except Exception as e:
            logger.warning("爬取文章失敗 %s: %s", url, e)

    if not results:
        logger.info("PTT 股版 %s 無符合文章（全文解析後）", date)
        return _gen_empty_df()

    df = pd.DataFrame(results)
    # 空字串 Time 轉為 None（MySQL TIME 欄位不接受空字串）
    df["Time"] = df["Time"].replace("", None)

    columns = ["Date", "Time", "Author", "Head", "url", "Content"]
    df = df[columns]

    logger.info("PTT 股版爬蟲完成: %d 篇文章", len(df))
    return df


def _gen_empty_df() -> pd.DataFrame:
    """產生空的 PTT 新聞 DataFrame。

    Returns:
        具有正確欄位的空 DataFrame。
    """
    return pd.DataFrame(
        columns=["Date", "Time", "Author", "Head", "url", "Content"]
    )
