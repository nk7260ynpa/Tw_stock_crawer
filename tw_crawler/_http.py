"""共用 HTTP 韌性工具（套件私有，不在 __init__.py re-export）。

集中處理「暫時性網路錯誤的指數退避重試」與「POST 後的狀態碼／JSON 驗證」，
讓各爬蟲模組能以一致、低風險的方式增加韌性，減少把暫時性錯誤丟進上游
retry queue 的次數，並把錯誤訊息講清楚（區分「資料未發布／被限流」與「真故障」）。

設計參考自 ptt_news.py 既有的 ``_request_with_retry``（指數退避）。

對外提供：
- ``retry_call``：通用「呼叫 → 失敗指數退避重試 → 全敗丟出最後例外」。
- ``safe_post_json``：POST 後先檢查 status_code 再解析 JSON，非 2xx／非 JSON
  時 raise 帶內文節錄的清楚例外，內部以 ``retry_call`` 套退避重試。
- ``NonJsonResponseError``：非 2xx 或非 JSON 回應的例外型別。
- 常數 ``DEFAULT_RETRIES``、``DEFAULT_BASE_DELAY``、``REQUEST_TIMEOUT``。
"""

import logging
import time
from typing import Any, Callable, TypeVar

import requests

logger = logging.getLogger(__name__)

# 預設最多嘗試次數（含第一次嘗試）。
DEFAULT_RETRIES = 3
# 首次重試前的等待秒數，之後以 2 的次方遞增（1s, 2s, 4s …）。
DEFAULT_BASE_DELAY = 1.0
# HTTP 請求逾時秒數。
REQUEST_TIMEOUT = 30
# 錯誤訊息中節錄回應內文的最大字元數。
_BODY_SNIPPET_LIMIT = 200

T = TypeVar("T")


class NonJsonResponseError(Exception):
    """POST 回應非 2xx 或無法解析為 JSON 時拋出的例外。

    訊息中會節錄實際回應內文前段，讓 log 看得到來源實際回了什麼，
    而非不透明的 ``Expecting value: line 1 column 1 (char 0)``。
    """


def retry_call(
    func: Callable[[], T],
    *,
    retries: int = DEFAULT_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    context: str = "請求",
) -> T:
    """以指數退避重試呼叫 ``func``，全部失敗時丟出最後一個例外。

    通用於需要對暫時性失敗（連線中斷、SSL 錯誤、限流等）自動重試的情境。
    呼叫端通常以 ``lambda`` 或 ``functools.partial`` 把參數包好後傳入。

    Args:
        func: 無參數的可呼叫物件，實際執行欲重試的工作。
        retries: 最多嘗試次數（含第一次嘗試）。
        base_delay: 首次重試前的等待秒數，之後以 2 的次方遞增。
        exceptions: 要攔截並觸發重試的例外型別 tuple，預設攔截所有 Exception。
        context: 描述文字，用於 log 訊息（如「TPEX 上櫃股票資料」）。

    Returns:
        ``func()`` 的回傳值。

    Raises:
        BaseException: 當所有嘗試皆失敗時，丟出最後一次捕捉到的例外。
    """
    last_exc: BaseException | None = None
    for attempt in range(1, retries + 1):
        try:
            return func()
        except exceptions as exc:  # noqa: B902 - 例外型別由呼叫端指定
            last_exc = exc
            if attempt < retries:
                delay = base_delay * (2 ** (attempt - 1))
                logger.warning(
                    "%s失敗（第 %d/%d 次）：%s，%.1f 秒後重試",
                    context, attempt, retries, exc, delay,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "%s失敗（已嘗試 %d 次）：%s",
                    context, retries, exc,
                )
    # 迴圈至少執行一次，last_exc 必為非 None。
    raise last_exc  # type: ignore[misc]


def _snippet(response: Any) -> str:
    """節錄回應內文前段，壓縮空白後截斷至上限長度。

    Args:
        response: HTTP 回應物件（具備 ``text`` 屬性）。

    Returns:
        節錄後的內文字串；無法取得內文時回傳空字串。
    """
    try:
        text = response.text or ""
    except Exception:  # pragma: no cover - 取內文本身失敗的極端情況
        return ""
    # 壓縮連續空白／換行，避免 log 被 HTML 排版撐爆。
    text = " ".join(text.split())
    if len(text) > _BODY_SNIPPET_LIMIT:
        return text[:_BODY_SNIPPET_LIMIT] + "…"
    return text


def safe_post_json(
    scraper_or_session: Any,
    url: str,
    *,
    data: Any = None,
    context: str = "請求",
    retries: int = DEFAULT_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    **kwargs: Any,
) -> Any:
    """POST 並安全地解析 JSON，先檢查狀態碼再解析、內建退避重試。

    與直接 ``scraper.post(url).json()`` 的差異：

    1. POST 後**先檢查** ``status_code``，非 2xx 直接拋出帶內文節錄的
       ``NonJsonResponseError``。
    2. 再嘗試 ``.json()``，失敗時拋出帶內文節錄的清楚例外，而非不透明的
       ``Expecting value: line 1 column 1 (char 0)``。
    3. 整個流程以 ``retry_call`` 包住，對連線錯誤與非 2xx／非 JSON 回應
       （多為暫時性 WAF／限流／HTML 錯誤頁）套用指數退避重試。

    Args:
        scraper_or_session: 具備 ``post()`` 方法的物件（``requests.Session``
            或 ``cloudscraper`` 實例）。
        url: 請求 URL。
        data: POST 表單資料。
        context: 描述文字，用於 log 與錯誤訊息。
        retries: 最多嘗試次數（含第一次嘗試）。
        base_delay: 首次重試前的等待秒數。
        **kwargs: 透傳給 ``post()`` 的其他參數（如 ``headers``）；未指定
            ``timeout`` 時自動帶入 ``REQUEST_TIMEOUT``。

    Returns:
        解析後的 JSON（``dict`` 或 ``list``）。

    Raises:
        NonJsonResponseError: 回應非 2xx 或非 JSON（重試後仍失敗）。
        requests.RequestException: 連線層級錯誤（重試後仍失敗）。
    """
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)

    def _do() -> Any:
        response = scraper_or_session.post(url, data=data, **kwargs)
        status = getattr(response, "status_code", None)
        if status is not None and not 200 <= status < 300:
            raise NonJsonResponseError(
                f"{context}回應狀態碼 {status}（非 2xx），內文節錄：{_snippet(response)}"
            )
        try:
            return response.json()
        except ValueError as exc:
            # requests/cloudscraper 的 JSONDecodeError 亦為 ValueError 子類。
            raise NonJsonResponseError(
                f"{context}回應非 JSON，內文節錄：{_snippet(response)}"
            ) from exc

    return retry_call(
        _do,
        retries=retries,
        base_delay=base_delay,
        exceptions=(requests.RequestException, NonJsonResponseError),
        context=context,
    )
