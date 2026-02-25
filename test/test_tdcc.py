"""TDCC 集保戶股權分散表爬蟲測試模組。"""

import pandas as pd
from pytest_mock import MockerFixture

import tw_crawler.tdcc as tdcc

TDCC_SAMPLE_RESPONSE = [
    {
        "資料日期": "20260213",
        "證券代號": "2330",
        "持股分級": "1",
        "人數": "385,614",
        "股數": "168,515,831",
        "占集保庫存數比例%": "6.49",
    },
    {
        "資料日期": "20260213",
        "證券代號": "2330",
        "持股分級": "2",
        "人數": "164,626",
        "股數": "422,929,305",
        "占集保庫存數比例%": "16.30",
    },
    {
        "資料日期": "20260213",
        "證券代號": "2330",
        "持股分級": "16",
        "人數": "553,334",
        "股數": "25,933,612,078",
        "占集保庫存數比例%": "100.00",
    },
]


def test_en_columns() -> None:
    """測試 en_columns 回傳正確的英文欄位名稱列表。"""
    result = tdcc.en_columns()
    expect = [
        "SecurityCode",
        "HoldingLevel",
        "Holders",
        "Shares",
        "Percentage",
    ]
    assert result == expect


def test_zh2en_columns() -> None:
    """測試 zh2en_columns 回傳正確的中英文對照字典。"""
    result = tdcc.zh2en_columns()
    expect = {
        "證券代號": "SecurityCode",
        "持股分級": "HoldingLevel",
        "人數": "Holders",
        "股數": "Shares",
        "占集保庫存數比例%": "Percentage",
    }
    assert result == expect


def test_remove_comma() -> None:
    """測試 remove_comma 正確移除逗號。"""
    result = tdcc.remove_comma("1,234,567")
    expect = "1234567"
    assert result == expect


def test_post_process() -> None:
    """測試 post_process 正確轉換欄位與清洗資料。"""
    df = pd.DataFrame({
        "證券代號": ["2330"],
        "持股分級": ["1"],
        "人數": ["385,614"],
        "股數": ["168,515,831"],
        "占集保庫存數比例%": ["6.49"],
    })
    date = "2026-02-13"
    result = tdcc.post_process(df, date)
    expected = pd.DataFrame({
        "Date": pd.to_datetime(["2026-02-13"]),
        "SecurityCode": ["2330"],
        "HoldingLevel": [1],
        "Holders": [385614],
        "Shares": [168515831],
        "Percentage": [6.49],
    })
    pd.testing.assert_frame_equal(result, expected)


def test_gen_empty_date_df() -> None:
    """測試 gen_empty_date_df 產生正確的空 DataFrame。"""
    result = tdcc.gen_empty_date_df()
    expect = pd.DataFrame(columns=tdcc.en_columns())
    expect.insert(0, "Date", pd.NaT)
    pd.testing.assert_frame_equal(result, expect)


def test_fetch_tdcc_data(mocker: MockerFixture) -> None:
    """測試 fetch_tdcc_data 正確呼叫 API 並回傳結果。"""
    mock_response = mocker.Mock()
    mock_response.json.return_value = TDCC_SAMPLE_RESPONSE
    mock_response.raise_for_status = mocker.Mock()
    mocker.patch(
        "tw_crawler.tdcc.requests.get",
        return_value=mock_response,
    )
    result = tdcc.fetch_tdcc_data()
    assert result == TDCC_SAMPLE_RESPONSE
    mock_response.raise_for_status.assert_called_once()


def test_parse_tdcc_data() -> None:
    """測試 parse_tdcc_data 正確解析 JSON 為 DataFrame。"""
    result = tdcc.parse_tdcc_data(TDCC_SAMPLE_RESPONSE)
    assert not result.empty
    assert list(result.columns) == [
        "Date", "SecurityCode", "HoldingLevel",
        "Holders", "Shares", "Percentage",
    ]
    assert result["Date"].iloc[0] == pd.Timestamp("2026-02-13")
    assert result["SecurityCode"].iloc[0] == "2330"
    assert result["HoldingLevel"].iloc[0] == 1
    assert result["Holders"].iloc[0] == 385614
    assert result["Shares"].iloc[0] == 168515831
    assert result["Percentage"].iloc[0] == 6.49
    assert len(result) == 3


def test_parse_tdcc_data_empty() -> None:
    """測試空回應時回傳空 DataFrame。"""
    result = tdcc.parse_tdcc_data([])
    expect = pd.DataFrame(columns=tdcc.en_columns())
    expect.insert(0, "Date", pd.NaT)
    pd.testing.assert_frame_equal(result, expect)


def test_tdcc_crawler(mocker: MockerFixture) -> None:
    """測試 tdcc_crawler 完整爬蟲流程。"""
    mocker.patch(
        "tw_crawler.tdcc.fetch_tdcc_data",
        return_value=TDCC_SAMPLE_RESPONSE,
    )
    result = tdcc.tdcc_crawler("2026-02-13")
    assert not result.empty
    assert len(result) == 3
    assert result["Date"].iloc[0] == pd.Timestamp("2026-02-13")
    assert result["SecurityCode"].iloc[0] == "2330"
