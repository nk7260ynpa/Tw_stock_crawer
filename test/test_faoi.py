"""FAOI 三大法人爬蟲測試模組。"""

import pandas as pd
from pytest_mock import MockerFixture

import tw_crawler.faoi as faoi

FAOI_FIELDS = [
    '證券代號', '證券名稱',
    '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)',
    '外陸資買賣超股數(不含外資自營商)',
    '外資自營商買進股數', '外資自營商賣出股數', '外資自營商買賣超股數',
    '投信買進股數', '投信賣出股數', '投信買賣超股數',
    '自營商買賣超股數',
    '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)',
    '自營商買賣超股數(自行買賣)',
    '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買賣超股數(避險)',
    '三大法人買賣超股數',
]

FAOI_SAMPLE_ROW = [
    "2330", "台積電", "1,000", "500", "500", "200", "100",
    "100022", "300", "150", "150", "50", "400", "2,010,000",
    "200", "100", "50", "50", "800",
]


def test_en_columns() -> None:
    result = faoi.en_columns()
    expect = [
        "SecurityCode",
        "StockName",
        "ForeignInvestorsTotalBuy",
        "ForeignInvestorsTotalSell",
        "ForeignInvestorsDifference",
        "ForeignDealersTotalBuy",
        "ForeignDealersTotalSell",
        "ForeignDealersDifference",
        "SecuritiesInvestmentTotalBuy",
        "SecuritiesInvestmentTotalSell",
        "SecuritiesInvestmentDifference",
        "DealersDifference",
        "DealersProprietaryTotalBuy",
        "DealersProprietaryTotalSell",
        "DealersProprietaryDifference",
        "DealersHedgeTotalBuy",
        "DealersHedgeTotalSell",
        "DealersHedgeDifference",
        "TotalDifference"
    ]
    assert result == expect


def test_zh2en_columns() -> None:
    result = faoi.zh2en_columns()
    expect = {
        "證券代號": "SecurityCode",
        "證券名稱": "StockName",
        "外陸資買進股數(不含外資自營商)": "ForeignInvestorsTotalBuy",
        "外陸資賣出股數(不含外資自營商)": "ForeignInvestorsTotalSell",
        "外陸資買賣超股數(不含外資自營商)": "ForeignInvestorsDifference",
        "外資自營商買進股數": "ForeignDealersTotalBuy",
        "外資自營商賣出股數": "ForeignDealersTotalSell",
        "外資自營商買賣超股數": "ForeignDealersDifference",
        "投信買進股數": "SecuritiesInvestmentTotalBuy",
        "投信賣出股數": "SecuritiesInvestmentTotalSell",
        "投信買賣超股數": "SecuritiesInvestmentDifference",
        "自營商買賣超股數": "DealersDifference",
        "自營商買進股數(自行買賣)": "DealersProprietaryTotalBuy",
        "自營商賣出股數(自行買賣)": "DealersProprietaryTotalSell",
        "自營商買賣超股數(自行買賣)": "DealersProprietaryDifference",
        "自營商買進股數(避險)": "DealersHedgeTotalBuy",
        "自營商賣出股數(避險)": "DealersHedgeTotalSell",
        "自營商買賣超股數(避險)": "DealersHedgeDifference",
        "三大法人買賣超股數": "TotalDifference"
    }
    assert result == expect


def test_remove_comma() -> None:
    result = faoi.remove_comma("1,234,567")
    expect = "1234567"
    assert result == expect


def test_post_process() -> None:
    df = pd.DataFrame({
        "SecurityCode": ["2330"],
        "StockName": ["台積電"],
        "ForeignInvestorsTotalBuy": ["1,000"],
        "ForeignInvestorsTotalSell": ["500"],
        "ForeignInvestorsDifference": ["500"],
        "ForeignDealersTotalBuy": ["200"],
        "ForeignDealersTotalSell": ["100"],
        "ForeignDealersDifference": ["100,0022"],
        "SecuritiesInvestmentTotalBuy": ["300"],
        "SecuritiesInvestmentTotalSell": ["150"],
        "SecuritiesInvestmentDifference": ["150"],
        "DealersDifference": ["50"],
        "DealersProprietaryTotalBuy": ["400"],
        "DealersProprietaryTotalSell": ["2,010,000"],
        "DealersProprietaryDifference": ["200"],
        "DealersHedgeTotalBuy": ["100"],
        "DealersHedgeTotalSell": ["50"],
        "DealersHedgeDifference": ["50"],
        "TotalDifference": ["800"]
    })
    date = pd.to_datetime("2024-10-29")
    result = faoi.post_process(df, date)
    expected = pd.DataFrame({
        "Date": [date],
        "SecurityCode": ["2330"],
        "StockName": ["台積電"],
        "ForeignInvestorsTotalBuy": [1000],
        "ForeignInvestorsTotalSell": [500],
        "ForeignInvestorsDifference": [500],
        "ForeignDealersTotalBuy": [200],
        "ForeignDealersTotalSell": [100],
        "ForeignDealersDifference": [1000022],
        "SecuritiesInvestmentTotalBuy": [300],
        "SecuritiesInvestmentTotalSell": [150],
        "SecuritiesInvestmentDifference": [150],
        "DealersDifference": [50],
        "DealersProprietaryTotalBuy": [400],
        "DealersProprietaryTotalSell": [2010000],
        "DealersProprietaryDifference": [200],
        "DealersHedgeTotalBuy": [100],
        "DealersHedgeTotalSell": [50],
        "DealersHedgeDifference": [50],
        "TotalDifference": [800]
    })
    pd.testing.assert_frame_equal(result, expected)


def test_gen_empty_date_df() -> None:
    result = faoi.gen_empty_date_df()
    expect = pd.DataFrame(columns=faoi.en_columns())
    expect.insert(0, "Date", pd.NaT)
    pd.testing.assert_frame_equal(result, expect)


def test_fetch_faoi_data(mocker: MockerFixture) -> None:
    mock_response = {
        "stat": "OK",
        "fields": FAOI_FIELDS,
        "data": FAOI_SAMPLE_ROW,
    }
    mocker.patch(
        'tw_crawler.faoi.requests.get',
        return_value=mocker.Mock(json=lambda: mock_response),
    )
    result = faoi.fetch_faoi_data("2022-02-18")
    assert result == mock_response


def test_parse_faoi_data() -> None:
    response = {
        "stat": "OK",
        "fields": FAOI_FIELDS,
        "data": [FAOI_SAMPLE_ROW],
    }
    date = "2022-02-18"
    result = faoi.parse_faoi_data(response, date)
    expect = pd.DataFrame({
        "Date": pd.to_datetime([date]),
        "SecurityCode": ["2330"],
        "StockName": ["台積電"],
        "ForeignInvestorsTotalBuy": [1000],
        "ForeignInvestorsTotalSell": [500],
        "ForeignInvestorsDifference": [500],
        "ForeignDealersTotalBuy": [200],
        "ForeignDealersTotalSell": [100],
        "ForeignDealersDifference": [100022],
        "SecuritiesInvestmentTotalBuy": [300],
        "SecuritiesInvestmentTotalSell": [150],
        "SecuritiesInvestmentDifference": [150],
        "DealersDifference": [50],
        "DealersProprietaryTotalBuy": [400],
        "DealersProprietaryTotalSell": [2010000],
        "DealersProprietaryDifference": [200],
        "DealersHedgeTotalBuy": [100],
        "DealersHedgeTotalSell": [50],
        "DealersHedgeDifference": [50],
        "TotalDifference": [800]
    })
    pd.testing.assert_frame_equal(result, expect)
    response = {
        'stat': '很抱歉，沒有符合條件的資料!',
        'total': 0,
    }
    result = faoi.parse_faoi_data(response, date)
    expect = pd.DataFrame(columns=faoi.en_columns())
    expect.insert(0, "Date", pd.NaT)
    pd.testing.assert_frame_equal(result, expect)


def test_faoi_crawler(mocker: MockerFixture) -> None:
    mock_response = {
        "stat": "OK",
        "fields": FAOI_FIELDS,
        "data": [FAOI_SAMPLE_ROW],
    }
    mocker.patch(
        'tw_crawler.faoi.fetch_faoi_data',
        return_value=mock_response,
    )
    result = faoi.faoi_crawler("2024-10-29")
    assert not result.empty
    expect = pd.DataFrame({
        "Date": pd.to_datetime(["2024-10-29"]),
        "SecurityCode": ["2330"],
        "StockName": ["台積電"],
        "ForeignInvestorsTotalBuy": [1000],
        "ForeignInvestorsTotalSell": [500],
        "ForeignInvestorsDifference": [500],
        "ForeignDealersTotalBuy": [200],
        "ForeignDealersTotalSell": [100],
        "ForeignDealersDifference": [100022],
        "SecuritiesInvestmentTotalBuy": [300],
        "SecuritiesInvestmentTotalSell": [150],
        "SecuritiesInvestmentDifference": [150],
        "DealersDifference": [50],
        "DealersProprietaryTotalBuy": [400],
        "DealersProprietaryTotalSell": [2010000],
        "DealersProprietaryDifference": [200],
        "DealersHedgeTotalBuy": [100],
        "DealersHedgeTotalSell": [50],
        "DealersHedgeDifference": [50],
        "TotalDifference": [800]
    })
    pd.testing.assert_frame_equal(result, expect)
