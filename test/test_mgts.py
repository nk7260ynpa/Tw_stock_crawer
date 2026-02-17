"""MGTS 融資融券爬蟲測試模組。"""

import pandas as pd
from pytest_mock import MockerFixture

import tw_crawler.mgts as mgts

MGTS_FIELDS = [
    "代號", "名稱", "融資買進", "融資賣出", "融資現金償還",
    "融資前日餘額", "融資當日餘額", "融資隔日限額",
    "融券買進", "融券賣出", "融券現券償還",
    "融券前日餘額", "融券當日餘額", "融券隔日限額",
    "資券互抵", "註記",
]

MGTS_SAMPLE_ROW = [
    "2330", "台積電", "1,000", "500", "100",
    "10,000", "10,400", "50,000",
    "200", "300", "50",
    "2,000", "2,050", "20,000",
    "150", "",
]


def test_en_columns() -> None:
    """測試 en_columns 回傳正確的英文欄位名稱列表。"""
    result = mgts.en_columns()
    expect = [
        "SecurityCode",
        "StockName",
        "MarginPurchase",
        "MarginSales",
        "CashRedemption",
        "MarginPurchaseBalanceOfPreviousDay",
        "MarginPurchaseBalanceOfTheDay",
        "MarginPurchaseQuotaForTheNextDay",
        "ShortCovering",
        "ShortSale",
        "StockRedemption",
        "ShortSaleBalanceOfPreviousDay",
        "ShortSaleBalanceOfTheDay",
        "ShortSaleQuotaForTheNextDay",
        "OffsettingOfMarginPurchasesAndShortSales",
        "Note"
    ]
    assert result == expect


def test_zh2en_columns() -> None:
    """測試 zh2en_columns 回傳正確的中英文對照字典。"""
    result = mgts.zh2en_columns()
    expect = {
        "日期": "Date",
        "代號": "SecurityCode",
        "名稱": "StockName",
        "融資買進": "MarginPurchase",
        "融資賣出": "MarginSales",
        "融資現金償還": "CashRedemption",
        "融資前日餘額": "MarginPurchaseBalanceOfPreviousDay",
        "融資當日餘額": "MarginPurchaseBalanceOfTheDay",
        "融資隔日限額": "MarginPurchaseQuotaForTheNextDay",
        "融券買進": "ShortCovering",
        "融券賣出": "ShortSale",
        "融券現券償還": "StockRedemption",
        "融券前日餘額": "ShortSaleBalanceOfPreviousDay",
        "融券當日餘額": "ShortSaleBalanceOfTheDay",
        "融券隔日限額": "ShortSaleQuotaForTheNextDay",
        "資券互抵": "OffsettingOfMarginPurchasesAndShortSales",
        "註記": "Note"
    }
    assert result == expect


def test_remove_comma() -> None:
    """測試 remove_comma 正確移除逗號。"""
    result = mgts.remove_comma("1,234,567")
    expect = "1234567"
    assert result == expect


def test_post_process() -> None:
    """測試 post_process 正確轉換欄位與清洗資料。"""
    df = pd.DataFrame(
        columns=MGTS_FIELDS,
        data=[MGTS_SAMPLE_ROW],
    )
    date = "2024-10-29"
    result = mgts.post_process(df, date)
    expected = pd.DataFrame({
        "Date": pd.to_datetime([date]),
        "SecurityCode": ["2330"],
        "StockName": ["台積電"],
        "MarginPurchase": [1000],
        "MarginSales": [500],
        "CashRedemption": [100],
        "MarginPurchaseBalanceOfPreviousDay": [10000],
        "MarginPurchaseBalanceOfTheDay": [10400],
        "MarginPurchaseQuotaForTheNextDay": [50000],
        "ShortCovering": [200],
        "ShortSale": [300],
        "StockRedemption": [50],
        "ShortSaleBalanceOfPreviousDay": [2000],
        "ShortSaleBalanceOfTheDay": [2050],
        "ShortSaleQuotaForTheNextDay": [20000],
        "OffsettingOfMarginPurchasesAndShortSales": [150],
        "Note": [""],
    })
    pd.testing.assert_frame_equal(result, expected)


def test_gen_empty_date_df() -> None:
    """測試 gen_empty_date_df 產生正確的空 DataFrame。"""
    result = mgts.gen_empty_date_df()
    expect = pd.DataFrame(columns=mgts.en_columns())
    expect.insert(0, "Date", pd.NaT)
    pd.testing.assert_frame_equal(result, expect)


def test_fetch_mgts_data(mocker: MockerFixture) -> None:
    """測試 fetch_mgts_data 正確呼叫 API 並回傳結果。"""
    mock_response = {
        "stat": "OK",
        "tables": [
            {},
            {
                "fields": MGTS_FIELDS,
                "data": [MGTS_SAMPLE_ROW],
            },
        ],
    }
    mocker.patch(
        "tw_crawler.mgts.requests.get",
        return_value=mocker.Mock(json=lambda: mock_response),
    )
    result = mgts.fetch_mgts_data("2024-10-29")
    assert result == mock_response


def test_parse_mgts_data() -> None:
    """測試 parse_mgts_data 正確解析 JSON 為 DataFrame。"""
    response = {
        "stat": "OK",
        "tables": [
            {},
            {
                "fields": MGTS_FIELDS,
                "data": [MGTS_SAMPLE_ROW],
            },
        ],
    }
    date = "2024-10-29"
    result = mgts.parse_mgts_data(response, date)
    expected = pd.DataFrame({
        "Date": pd.to_datetime([date]),
        "SecurityCode": ["2330"],
        "StockName": ["台積電"],
        "MarginPurchase": [1000],
        "MarginSales": [500],
        "CashRedemption": [100],
        "MarginPurchaseBalanceOfPreviousDay": [10000],
        "MarginPurchaseBalanceOfTheDay": [10400],
        "MarginPurchaseQuotaForTheNextDay": [50000],
        "ShortCovering": [200],
        "ShortSale": [300],
        "StockRedemption": [50],
        "ShortSaleBalanceOfPreviousDay": [2000],
        "ShortSaleBalanceOfTheDay": [2050],
        "ShortSaleQuotaForTheNextDay": [20000],
        "OffsettingOfMarginPurchasesAndShortSales": [150],
        "Note": [""],
    })
    pd.testing.assert_frame_equal(result, expected)

    # 測試休市情況
    response_empty = {
        "stat": "很抱歉，沒有符合條件的資料!",
        "total": 0,
    }
    result_empty = mgts.parse_mgts_data(response_empty, date)
    expect_empty = pd.DataFrame(columns=mgts.en_columns())
    expect_empty.insert(0, "Date", pd.NaT)
    pd.testing.assert_frame_equal(result_empty, expect_empty)


def test_mgts_crawler(mocker: MockerFixture) -> None:
    """測試 mgts_crawler 完整爬蟲流程。"""
    mock_response = {
        "stat": "OK",
        "tables": [
            {},
            {
                "fields": MGTS_FIELDS,
                "data": [MGTS_SAMPLE_ROW],
            },
        ],
    }
    mocker.patch(
        "tw_crawler.mgts.fetch_mgts_data",
        return_value=mock_response,
    )
    result = mgts.mgts_crawler("2024-10-29")
    assert not result.empty
    expected = pd.DataFrame({
        "Date": pd.to_datetime(["2024-10-29"]),
        "SecurityCode": ["2330"],
        "StockName": ["台積電"],
        "MarginPurchase": [1000],
        "MarginSales": [500],
        "CashRedemption": [100],
        "MarginPurchaseBalanceOfPreviousDay": [10000],
        "MarginPurchaseBalanceOfTheDay": [10400],
        "MarginPurchaseQuotaForTheNextDay": [50000],
        "ShortCovering": [200],
        "ShortSale": [300],
        "StockRedemption": [50],
        "ShortSaleBalanceOfPreviousDay": [2000],
        "ShortSaleBalanceOfTheDay": [2050],
        "ShortSaleQuotaForTheNextDay": [20000],
        "OffsettingOfMarginPurchasesAndShortSales": [150],
        "Note": [""],
    })
    pd.testing.assert_frame_equal(result, expected)
