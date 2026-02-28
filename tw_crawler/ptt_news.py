"""PTT 股版新聞爬蟲模組。

提供 PTT Stock 版（批踢踢股票版）文章爬取與處理功能。
使用 requests + BeautifulSoup 爬取列表頁與文章頁，
markdownify 將內文轉為 Markdown。

支援兩種模式：
- 日期模式（date）：抓取指定日期的全部文章
- 時數模式（hours）：抓取過去 N 小時內的文章，避免排程間隔漏抓
"""

import logging
import re
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md

# 台灣時區 (UTC+8)
TW_TZ = timezone(timedelta(hours=8))

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
    cutoff_date: "datetime.date | None" = None,
) -> tuple[list[dict], bool]:
    """從列表頁解析文章資訊，篩選符合條件的文章。

    支援兩種模式：
    - 日期模式（cutoff_date=None）：篩選指定日期的文章
    - 時數模式（cutoff_date 有值）：篩選 cutoff_date 當日及之後的文章

    注意：PTT 列表頁僅有日期（無時間），時數模式下以日期寬鬆篩選，
    精確時間篩選在文章全文爬取後執行。

    Args:
        soup: 列表頁的 BeautifulSoup 物件。
        target_date: 目標日期字串（YYYY-MM-DD）。
        target_year: 目標查詢年份（用於補足 PTT 列表日期的年份）。
        cutoff_date: 時數模式下的截止日期，篩選此日期（含）之後的文章。

    Returns:
        tuple: (符合條件的文章清單, 是否發現更早的文章)。
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
                if cutoff_date is not None:
                    # 時數模式：篩選 cutoff_date 當日及之後
                    if article_date >= cutoff_date:
                        articles.append({
                            "url": url,
                            "title": title,
                            "author": author,
                        })
                    elif article_date < cutoff_date:
                        found_older = True
                else:
                    # 日期模式
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


def ptt_news_crawler(
    date: str,
    hours: int | None = None,
) -> pd.DataFrame:
    """爬取 PTT 股版文章。

    支援兩種模式：
    - 日期模式（hours=None）：抓取指定日期的全部文章
    - 時數模式（hours 有值）：抓取過去 N 小時內的文章

    流程：
    1. 從 PTT Stock 版最新頁開始
    2. 依模式篩選文章（日期或時間區間）
    3. 逐篇爬取文章頁面取得完整時間與全文
    4. 使用 markdownify 將內文轉為 Markdown
    5. 時數模式下用完整時間做精確篩選
    6. 遇到超出範圍的更早文章時停止翻頁

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。
        hours: 抓取過去幾小時的文章。若為 None 則使用日期模式。

    Returns:
        包含 Date, Time, Author, Head, url, Content
        欄位的 DataFrame。若無符合條件的文章則回傳空 DataFrame。
    """
    # 計算時間截止點（時數模式）
    cutoff_dt = None
    cutoff_date = None
    if hours is not None:
        now = datetime.now(tz=TW_TZ)
        cutoff_dt = now - timedelta(hours=hours)
        cutoff_date = cutoff_dt.date()
        logger.info(
            "開始 PTT 股版爬蟲（時數模式）: 過去 %d 小時 "
            "（截止時間: %s）",
            hours, cutoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
        )
    else:
        logger.info("開始 PTT 股版爬蟲（日期模式）: %s", date)

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
            soup, date, target_year, cutoff_date=cutoff_date,
        )
        all_candidates.extend(matched)
        logger.info(
            "PTT 列表頁 %s: %d 篇符合條件",
            current_url.split("/")[-1], len(matched),
        )

        if found_older:
            logger.info("發現超出範圍的更早文章，停止翻頁")
            break

        current_url = prev_url
        if current_url:
            time.sleep(REQUEST_DELAY)

    if not all_candidates:
        logger.info("PTT 股版無符合條件的文章")
        return _gen_empty_df()

    # 逐篇爬取文章全文
    target_dt = datetime.strptime(date, "%Y-%m-%d").date()
    naive_cutoff = cutoff_dt.replace(tzinfo=None) if cutoff_dt else None
    results = []
    for candidate in all_candidates:
        url = candidate["url"]
        if not url:
            continue

        try:
            time.sleep(REQUEST_DELAY)
            detail = fetch_article_detail(session, url)

            # 用文章頁面的完整時間進行篩選
            full_dt = detail.get("full_datetime")
            if full_dt:
                if naive_cutoff is not None:
                    # 時數模式：精確篩選
                    if full_dt < naive_cutoff:
                        logger.debug(
                            "文章時間 %s 早於截止時間，跳過: %s",
                            full_dt, url,
                        )
                        continue
                else:
                    # 日期模式：驗證日期
                    if full_dt.date() != target_dt:
                        logger.debug(
                            "文章實際日期 %s 與目標 %s 不符，跳過: %s",
                            full_dt.date(), target_dt, url,
                        )
                        continue

            # 決定文章日期
            article_date_str = (
                full_dt.strftime("%Y-%m-%d") if full_dt else date
            )

            results.append({
                "Date": article_date_str,
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
        logger.info("PTT 股版無符合條件的文章（全文解析後）")
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
