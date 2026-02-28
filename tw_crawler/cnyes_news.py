"""鉅亨網台股新聞爬蟲模組。

提供鉅亨網(CNYES)台股新聞文章爬取與處理功能。
使用 CNYES 公開 JSON API 取得新聞列表與內文。

支援兩種模式：
- 日期模式（date）：抓取指定日期的全部新聞
- 時數模式（hours）：抓取過去 N 小時內的新聞，避免排程間隔漏抓
"""

import html
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from markdownify import markdownify as md

logger = logging.getLogger(__name__)

# CNYES 台股新聞 API URL
CNYES_API_URL = (
    "https://api.cnyes.com/media/api/v1/newslist/category/tw_stock"
)

# 台灣時區 (UTC+8)
TW_TZ = timezone(timedelta(hours=8))

# 每次 HTTP 請求的逾時秒數
REQUEST_TIMEOUT = 30


def _timestamp_to_tw_datetime(ts: int) -> datetime:
    """將 Unix timestamp 轉換為台灣時區的 datetime。

    Args:
        ts: Unix timestamp（秒）。

    Returns:
        台灣時區的 datetime 物件。
    """
    return datetime.fromtimestamp(ts, tz=TW_TZ)


def _html_to_markdown(html_content: str) -> str:
    """將 HTML 內容轉換為 Markdown 純文字。

    Args:
        html_content: HTML 格式的文章內容。

    Returns:
        轉換後的 Markdown 字串，已去除首尾空白。
    """
    if not html_content:
        return ""
    # CNYES API 回傳的 content 使用 HTML entities 編碼（&lt;p&gt; 而非 <p>），
    # 需先解碼才能讓 markdownify 正確辨識 HTML 標籤。
    unescaped = html.unescape(html_content)
    return md(unescaped, strip=["img", "script", "style"]).strip()


def _keywords_to_hashtag(keywords: list) -> str:
    """將 keyword 陣列轉為逗號分隔的 HashTag 字串。

    Args:
        keywords: 關鍵字列表。

    Returns:
        逗號分隔的 HashTag 字串。空列表回傳空字串。
    """
    if not keywords:
        return ""
    return ",".join(str(k) for k in keywords if k)


def _build_article_url(news_id: int) -> str:
    """根據 newsId 組成鉅亨網文章 URL。

    Args:
        news_id: 鉅亨網新聞 ID。

    Returns:
        完整的文章 URL。
    """
    return f"https://news.cnyes.com/news/id/{news_id}"


def fetch_news_page(page: int) -> dict:
    """取得 CNYES 台股新聞 API 的指定頁面。

    Args:
        page: 頁碼（從 1 開始）。

    Returns:
        API 回應的 JSON 字典。

    Raises:
        requests.RequestException: 當 HTTP 請求失敗時。
        ValueError: 當回應非預期格式時。
    """
    logger.info("取得 CNYES 新聞 API 第 %d 頁", page)
    response = requests.get(
        CNYES_API_URL,
        params={"page": page},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()

    if data.get("statusCode") != 200:
        raise ValueError(
            f"CNYES API 回傳非 200 狀態碼: {data.get('statusCode')}"
        )

    return data


def parse_news_items(
    items: list[dict],
    target_date: str,
    cutoff_dt: datetime | None = None,
) -> tuple[list[dict], bool]:
    """從 API 回應的新聞列表中篩選文章。

    支援兩種篩選模式：
    - 日期模式（cutoff_dt=None）：篩選指定日期的文章
    - 時數模式（cutoff_dt 有值）：篩選 cutoff_dt 之後的文章

    Args:
        items: API 回傳的新聞資料列表。
        target_date: 目標日期字串（YYYY-MM-DD），日期模式下用於精確篩選。
        cutoff_dt: 時間截止點（含時區），時數模式下篩選此時間之後的文章。

    Returns:
        tuple: (符合條件的文章清單, 是否發現超出範圍的更早文章)。
            文章清單中每筆資料包含 Date, Time, Author, Head, HashTag,
            url, Content 欄位。
    """
    target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    matched = []
    found_older = False

    for item in items:
        publish_at = item.get("publishAt")
        if publish_at is None:
            logger.warning("新聞項目缺少 publishAt 欄位: %s", item.get("newsId"))
            continue

        article_dt = _timestamp_to_tw_datetime(publish_at)

        if cutoff_dt is not None:
            # 時數模式：篩選 cutoff_dt 之後的文章
            if article_dt >= cutoff_dt:
                news_id = item.get("newsId", 0)
                matched.append({
                    "Date": article_dt.strftime("%Y-%m-%d"),
                    "Time": article_dt.strftime("%H:%M:%S"),
                    "Author": item.get("author", "") or "",
                    "Head": item.get("title", ""),
                    "HashTag": _keywords_to_hashtag(
                        item.get("keyword", [])
                    ),
                    "url": _build_article_url(news_id),
                    "Content": _html_to_markdown(
                        item.get("content", "")
                    ),
                })
            else:
                found_older = True
        else:
            # 日期模式：篩選指定日期的文章
            article_date = article_dt.date()
            if article_date == target_dt:
                news_id = item.get("newsId", 0)
                matched.append({
                    "Date": target_date,
                    "Time": article_dt.strftime("%H:%M:%S"),
                    "Author": item.get("author", "") or "",
                    "Head": item.get("title", ""),
                    "HashTag": _keywords_to_hashtag(
                        item.get("keyword", [])
                    ),
                    "url": _build_article_url(news_id),
                    "Content": _html_to_markdown(
                        item.get("content", "")
                    ),
                })
            elif article_date < target_dt:
                found_older = True

    return matched, found_older


def cnyes_news_crawler(
    date: str,
    hours: int | None = None,
) -> pd.DataFrame:
    """爬取鉅亨網台股新聞。

    支援兩種模式：
    - 日期模式（hours=None）：抓取指定日期的全部新聞
    - 時數模式（hours 有值）：抓取過去 N 小時內的新聞

    流程：
    1. 從第 1 頁開始呼叫 CNYES JSON API
    2. 依模式篩選文章（日期或時間區間）
    3. 使用 markdownify 將 HTML content 轉為 Markdown
    4. 遇到超出範圍的更早文章時停止翻頁
    5. 回傳 DataFrame

    Args:
        date: 日期字串，格式為 'YYYY-MM-DD'。
        hours: 抓取過去幾小時的新聞。若為 None 則使用日期模式。

    Returns:
        包含 Date, Time, Author, Head, HashTag, url, Content
        欄位的 DataFrame。若無符合條件的新聞則回傳空 DataFrame。
    """
    # 計算時間截止點（時數模式）
    cutoff_dt = None
    if hours is not None:
        now = datetime.now(tz=TW_TZ)
        cutoff_dt = now - timedelta(hours=hours)
        logger.info(
            "開始 CNYES 新聞爬蟲（時數模式）: 過去 %d 小時 "
            "（截止時間: %s）",
            hours, cutoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
        )
    else:
        logger.info("開始 CNYES 新聞爬蟲（日期模式）: %s", date)

    all_articles = []
    page = 1
    last_page = 1  # 先設為 1，後續從 API 回應更新

    while page <= last_page:
        try:
            data = fetch_news_page(page)
        except (requests.RequestException, ValueError) as e:
            logger.error("取得 CNYES API 第 %d 頁失敗: %s", page, e)
            break

        items_data = data.get("items", {})
        news_list = items_data.get("data", [])

        if not news_list:
            logger.info("CNYES API 第 %d 頁無資料，停止翻頁", page)
            break

        # 更新最大頁數（僅第一頁需要）
        if page == 1:
            last_page = items_data.get("last_page", 1)
            logger.info("CNYES API 共 %d 頁", last_page)

        matched, found_older = parse_news_items(
            news_list, date, cutoff_dt=cutoff_dt,
        )
        all_articles.extend(matched)
        logger.info(
            "CNYES API 第 %d 頁: %d/%d 篇符合條件",
            page, len(matched), len(news_list),
        )

        if found_older:
            logger.info("發現超出範圍的更早文章，停止翻頁")
            break

        page += 1

    if not all_articles:
        logger.info("CNYES 新聞無符合條件的文章")
        return _gen_empty_df()

    df = pd.DataFrame(all_articles)
    # 空字串 Time 轉為 None（MySQL TIME 欄位不接受空字串）
    df["Time"] = df["Time"].replace("", None)

    columns = ["Date", "Time", "Author", "Head", "HashTag", "url", "Content"]
    df = df[columns]

    logger.info("CNYES 新聞爬蟲完成: %d 篇文章", len(df))
    return df


def _gen_empty_df() -> pd.DataFrame:
    """產生空的 CNYES 新聞 DataFrame。

    Returns:
        具有正確欄位的空 DataFrame。
    """
    return pd.DataFrame(
        columns=["Date", "Time", "Author", "Head", "HashTag", "url", "Content"]
    )
