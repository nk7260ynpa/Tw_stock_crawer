"""共用 HTTP 韌性工具（tw_crawler/_http.py）測試模組。"""

import pytest
import requests
from pytest_mock import MockerFixture

from tw_crawler._http import (
    DEFAULT_BASE_DELAY,
    DEFAULT_RETRIES,
    REQUEST_TIMEOUT,
    NonJsonResponseError,
    retry_call,
    safe_post_json,
)


# --- 常數 ---


def test_default_constants() -> None:
    """確認預設常數存在且型別合理。"""
    assert DEFAULT_RETRIES >= 1
    assert DEFAULT_BASE_DELAY > 0
    assert REQUEST_TIMEOUT > 0


# --- retry_call ---


def test_retry_call_success_first_try(mocker: MockerFixture) -> None:
    """第一次即成功時不應重試、不應 sleep。"""
    sleep = mocker.patch("tw_crawler._http.time.sleep")
    func = mocker.Mock(return_value="ok")

    result = retry_call(func, context="測試")

    assert result == "ok"
    func.assert_called_once()
    sleep.assert_not_called()


def test_retry_call_retries_then_succeeds(mocker: MockerFixture) -> None:
    """前兩次失敗、第三次成功時應回傳成功結果並重試兩次。"""
    sleep = mocker.patch("tw_crawler._http.time.sleep")
    func = mocker.Mock(side_effect=[ValueError("x"), ValueError("y"), "ok"])

    result = retry_call(func, retries=3, base_delay=1.0, context="測試")

    assert result == "ok"
    assert func.call_count == 3
    # 指數退避：第 1 次失敗等 1s、第 2 次失敗等 2s。
    assert [c.args[0] for c in sleep.call_args_list] == [1.0, 2.0]


def test_retry_call_all_fail_raises_last(mocker: MockerFixture) -> None:
    """全部失敗時應丟出最後一次的例外。"""
    mocker.patch("tw_crawler._http.time.sleep")
    last_error = ValueError("最後一次")
    func = mocker.Mock(
        side_effect=[ValueError("第一次"), ValueError("第二次"), last_error],
    )

    with pytest.raises(ValueError, match="最後一次"):
        retry_call(func, retries=3, context="測試")

    assert func.call_count == 3


def test_retry_call_only_catches_listed_exceptions(
    mocker: MockerFixture,
) -> None:
    """未列入 exceptions 的例外應立即向外拋出、不重試。"""
    sleep = mocker.patch("tw_crawler._http.time.sleep")
    func = mocker.Mock(side_effect=KeyError("未攔截"))

    with pytest.raises(KeyError):
        retry_call(func, exceptions=(ValueError,), context="測試")

    func.assert_called_once()
    sleep.assert_not_called()


# --- safe_post_json ---


def _mock_post_result(mocker: MockerFixture, *, status_code=200, text="", json=None):
    """建立模擬的 POST 回應物件。"""
    result = mocker.Mock()
    result.status_code = status_code
    result.text = text
    if isinstance(json, Exception):
        result.json.side_effect = json
    else:
        result.json.return_value = json
    return result


def test_safe_post_json_success(mocker: MockerFixture) -> None:
    """正常 2xx + JSON 時回傳解析後的內容，並帶入預設 timeout。"""
    payload = {"ok": True, "value": 1}
    post_result = _mock_post_result(
        mocker, status_code=200, text="{}", json=payload,
    )
    scraper = mocker.Mock()
    scraper.post.return_value = post_result

    result = safe_post_json(scraper, "https://example.com", data={"a": 1})

    assert result == payload
    scraper.post.assert_called_once_with(
        "https://example.com", data={"a": 1}, timeout=REQUEST_TIMEOUT,
    )


def test_safe_post_json_non_json_raises_with_snippet(
    mocker: MockerFixture,
) -> None:
    """回應為非 JSON 時應拋出 NonJsonResponseError 並節錄回應內文。"""
    mocker.patch("tw_crawler._http.time.sleep")
    html = "<html><body>暫時無法服務 please retry later</body></html>"
    post_result = _mock_post_result(
        mocker,
        status_code=200,
        text=html,
        json=ValueError("Expecting value: line 1 column 1 (char 0)"),
    )
    scraper = mocker.Mock()
    scraper.post.return_value = post_result

    with pytest.raises(NonJsonResponseError) as exc_info:
        safe_post_json(scraper, "https://example.com", context="測試來源")

    message = str(exc_info.value)
    assert "測試來源" in message
    assert "非 JSON" in message
    assert "please retry later" in message  # 內文節錄可見
    # 重試到上限後才放棄。
    assert scraper.post.call_count == DEFAULT_RETRIES


def test_safe_post_json_non_2xx_raises_with_status(
    mocker: MockerFixture,
) -> None:
    """回應非 2xx 時應拋出帶狀態碼與內文節錄的清楚例外。"""
    mocker.patch("tw_crawler._http.time.sleep")
    post_result = _mock_post_result(
        mocker,
        status_code=503,
        text="Service Unavailable",
        json={"should": "not be used"},
    )
    scraper = mocker.Mock()
    scraper.post.return_value = post_result

    with pytest.raises(NonJsonResponseError) as exc_info:
        safe_post_json(scraper, "https://example.com", context="測試來源")

    message = str(exc_info.value)
    assert "503" in message
    assert "Service Unavailable" in message
    # 非 2xx 時不應呼叫 .json()
    post_result.json.assert_not_called()


def test_safe_post_json_retries_then_succeeds(
    mocker: MockerFixture,
) -> None:
    """連線錯誤一次後重試成功，應回傳 JSON。"""
    mocker.patch("tw_crawler._http.time.sleep")
    payload = {"ok": True}
    good_result = _mock_post_result(
        mocker, status_code=200, text="{}", json=payload,
    )
    scraper = mocker.Mock()
    scraper.post.side_effect = [
        requests.ConnectionError("DNS 暫時失敗"),
        good_result,
    ]

    result = safe_post_json(scraper, "https://example.com", context="測試來源")

    assert result == payload
    assert scraper.post.call_count == 2


def test_safe_post_json_long_body_snippet_truncated(
    mocker: MockerFixture,
) -> None:
    """過長的回應內文節錄應被截斷並加上省略號。"""
    mocker.patch("tw_crawler._http.time.sleep")
    long_text = "A" * 1000
    post_result = _mock_post_result(
        mocker,
        status_code=200,
        text=long_text,
        json=ValueError("not json"),
    )
    scraper = mocker.Mock()
    scraper.post.return_value = post_result

    with pytest.raises(NonJsonResponseError) as exc_info:
        safe_post_json(scraper, "https://example.com", context="測試來源")

    assert "…" in str(exc_info.value)
