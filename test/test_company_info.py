"""公司產業對照爬蟲測試模組。"""

import pandas as pd
from pytest_mock import MockerFixture

import tw_crawler.company_info as company_info


# --- 測試用假資料 ---

TWSE_SAMPLE_DATA = [
    {
        "出表日期": "113/01/01",
        "公司代號": "1101",
        "公司名稱": "臺灣水泥股份有限公司",
        "公司簡稱": "台泥",
        "外國企業註冊地國": "",
        "產業別": "01",
        "住址": "台北市中山區中山北路二段113號",
        "營利事業統一編號": "11111111",
        "董事長": "張安平",
        "總經理": "程耀輝",
        "發言人": "黃健強",
        "發言人職稱": "總經理室協理",
        "代理發言人": "",
        "總機電話": "02-25311099",
        "成立日期": "39/12/27",
        "上市日期": "51/02/09",
        "普通股每股面額": "新台幣 10.0000元",
        "實收資本額": "77231817420",
        "私募股數": "0",
        "特別股": "200000000",
        "編制財務報表類型": "合併",
        "股票過戶機構": "中國信託商業銀行",
        "過戶電話": "02-66365566",
        "過戶地址": "台北市重慶南路一段83號5樓",
        "簽證會計師事務所": "勤業眾信",
        "簽證會計師1": "林盈光",
        "簽證會計師2": "江美艷",
        "英文簡稱": "TCC",
        "英文通訊地址": "113 Zhongshan N. Rd.",
        "傳真機號碼": "02-25427516",
        "電子郵件信箱": "tcc@taiwancement.com",
        "網址": "https://www.taiwancement.com",
        "已發行普通股數或TDR原股發行股數": "7523181742",
    },
    {
        "出表日期": "113/01/01",
        "公司代號": "2330",
        "公司名稱": "台灣積體電路製造股份有限公司",
        "公司簡稱": "台積電",
        "外國企業註冊地國": "",
        "產業別": "24",
        "住址": "新竹科學園區力行六路8號",
        "營利事業統一編號": "22099131",
        "董事長": "魏哲家",
        "總經理": "魏哲家",
        "發言人": "黃仁昭",
        "發言人職稱": "財務長",
        "代理發言人": "",
        "總機電話": "03-5636688",
        "成立日期": "76/02/21",
        "上市日期": "83/09/05",
        "普通股每股面額": "新台幣 10.0000元",
        "實收資本額": "259303804580",
        "私募股數": "0",
        "特別股": "0",
        "編制財務報表類型": "合併",
        "股票過戶機構": "中國信託商業銀行",
        "過戶電話": "02-66365566",
        "過戶地址": "台北市重慶南路一段83號5樓",
        "簽證會計師事務所": "勤業眾信",
        "簽證會計師1": "陳建宇",
        "簽證會計師2": "曾冠豪",
        "英文簡稱": "TSMC",
        "英文通訊地址": "8 Li-Hsin Rd.",
        "傳真機號碼": "03-5637000",
        "電子郵件信箱": "tsmc@tsmc.com",
        "網址": "https://www.tsmc.com",
        "已發行普通股數或TDR原股發行股數": "25930380458",
    },
]

TPEX_SAMPLE_DATA = [
    {
        "Date": "113/01/01",
        "SecuritiesCompanyCode": "6488",
        "CompanyName": "環球晶圓股份有限公司",
        "CompanyAbbreviation": "環球晶",
        "Registration": "",
        "SecuritiesIndustryCode": "24",
        "Address": "新竹科學園區力行路19號",
        "UnifiedBusinessNo.": "28299870",
        "Chairman": "徐秀蘭",
        "GeneralManager": "徐秀蘭",
        "Spokesman": "劉博文",
        "TitleOfSpokesman": "財務長",
        "DeputySpokesperson": "",
        "Telephone": "03-5776988",
        "DateOfIncorporation": "100/06/08",
        "DateOfListing": "105/06/23",
        "ParValueOfCommonStock": "新台幣 10.0000元",
        "Paidin.Capital.NTDollars": "4356360380",
        "PrivateStock.shares": "0",
        "PreferredStock.shares": "0",
        "PreparationOfFinancialReportType": "合併",
        "StockTransferAgent": "中國信託商業銀行",
        "StockTransferAgentTelephone": "02-66365566",
        "StockTransferAgentAddress": "台北市重慶南路一段83號5樓",
        "AccountingFirm": "安侯建業",
        "CPA.CharteredPublicAccountant.First": "劉恩誌",
        "CPA.CharteredPublicAccountant.Second": "孫維德",
        "Symbol": "GWC",
        "Fax": "03-5776989",
        "EmailAddress": "ir@gw-semi.com",
        "WebAddress": "https://www.gw-semi.com",
        "IssueShares": "435636038",
    },
    {
        "Date": "113/01/01",
        "SecuritiesCompanyCode": "3105",
        "CompanyName": "穩懋半導體股份有限公司",
        "CompanyAbbreviation": "穩懋",
        "Registration": "",
        "SecuritiesIndustryCode": "24",
        "Address": "桃園市龜山區文化里文化二路28號5樓",
        "UnifiedBusinessNo.": "84149666",
        "Chairman": "陳進財",
        "GeneralManager": "王郁琦",
        "Spokesman": "林玫君",
        "TitleOfSpokesman": "財務長",
        "DeputySpokesperson": "",
        "Telephone": "03-3975888",
        "DateOfIncorporation": "88/03/25",
        "DateOfListing": "100/12/21",
        "ParValueOfCommonStock": "新台幣 10.0000元",
        "Paidin.Capital.NTDollars": "3529116700",
        "PrivateStock.shares": "0",
        "PreferredStock.shares": "0",
        "PreparationOfFinancialReportType": "合併",
        "StockTransferAgent": "凱基證券",
        "StockTransferAgentTelephone": "02-23892999",
        "StockTransferAgentAddress": "台北市中正區忠孝西路一段6號B2",
        "AccountingFirm": "安永",
        "CPA.CharteredPublicAccountant.First": "傅文芳",
        "CPA.CharteredPublicAccountant.Second": "何思吟",
        "Symbol": "WIN",
        "Fax": "03-3975999",
        "EmailAddress": "ir@winfoundry.com",
        "WebAddress": "https://www.winfoundry.com",
        "IssueShares": "352911670",
    },
]


# --- _parse_par_value 測試 ---


def test_parse_par_value_normal() -> None:
    """測試正常面額字串解析。"""
    result = company_info._parse_par_value("新台幣 10.0000元")
    assert result == 10.0


def test_parse_par_value_different_value() -> None:
    """測試不同面額數值的解析。"""
    result = company_info._parse_par_value("新台幣 1.0000元")
    assert result == 1.0


def test_parse_par_value_empty() -> None:
    """測試空字串回傳 None。"""
    result = company_info._parse_par_value("")
    assert result is None


def test_parse_par_value_none() -> None:
    """測試 None 輸入回傳 None。"""
    result = company_info._parse_par_value(None)
    assert result is None


def test_parse_par_value_no_number() -> None:
    """測試無數字字串回傳 None。"""
    result = company_info._parse_par_value("無面額")
    assert result is None


# --- _safe_int 測試 ---


def test_safe_int_normal() -> None:
    """測試正常數字字串轉換。"""
    assert company_info._safe_int("200000000") == 200000000


def test_safe_int_with_comma() -> None:
    """測試含逗號數字字串轉換。"""
    assert company_info._safe_int("1,000,000") == 1000000


def test_safe_int_empty() -> None:
    """測試空字串回傳 0。"""
    assert company_info._safe_int("") == 0


def test_safe_int_none() -> None:
    """測試 None 回傳 0。"""
    assert company_info._safe_int(None) == 0


def test_safe_int_invalid() -> None:
    """測試非數字字串回傳 0。"""
    assert company_info._safe_int("abc") == 0


def test_safe_int_float_string() -> None:
    """測試浮點數字串轉換為整數。"""
    assert company_info._safe_int("100.5") == 100


# --- _calculate_normal_shares 測試 ---


def test_calculate_normal_shares_normal() -> None:
    """測試正常普通股計算。"""
    # 77231817420 / 10 - 200000000 - 0 = 7523181742
    result = company_info._calculate_normal_shares(
        "77231817420", "新台幣 10.0000元", "200000000", "0"
    )
    assert result == 7523181742


def test_calculate_normal_shares_no_special() -> None:
    """測試無特別股與私募股的計算。"""
    # 259303804580 / 10 - 0 - 0 = 25930380458
    result = company_info._calculate_normal_shares(
        "259303804580", "新台幣 10.0000元", "0", "0"
    )
    assert result == 25930380458


def test_calculate_normal_shares_invalid_par_value() -> None:
    """測試面額無法解析時回傳 0。"""
    result = company_info._calculate_normal_shares(
        "100000000", "", "0", "0"
    )
    assert result == 0


def test_calculate_normal_shares_zero_par_value() -> None:
    """測試面額為 0 時回傳 0。"""
    result = company_info._calculate_normal_shares(
        "100000000", "新台幣 0元", "0", "0"
    )
    assert result == 0


# --- fetch 函式測試 ---


def test_fetch_twse_company_info(mocker: MockerFixture) -> None:
    """測試從 TWSE API 取得公司資料。"""
    mock_response = mocker.Mock()
    mock_response.json.return_value = TWSE_SAMPLE_DATA
    mock_response.raise_for_status = mocker.Mock()
    mocker.patch(
        "tw_crawler.company_info.requests.get",
        return_value=mock_response,
    )

    result = company_info.fetch_twse_company_info()

    assert result == TWSE_SAMPLE_DATA
    company_info.requests.get.assert_called_once_with(
        company_info.TWSE_API_URL, timeout=30
    )


def test_fetch_tpex_company_info(mocker: MockerFixture) -> None:
    """測試從 TPEX API 取得公司資料。"""
    mock_response = mocker.Mock()
    mock_response.json.return_value = TPEX_SAMPLE_DATA
    mock_response.raise_for_status = mocker.Mock()
    mocker.patch(
        "tw_crawler.company_info.requests.get",
        return_value=mock_response,
    )

    result = company_info.fetch_tpex_company_info()

    assert result == TPEX_SAMPLE_DATA
    company_info.requests.get.assert_called_once_with(
        company_info.TPEX_API_URL, timeout=30
    )


# --- parse 函式測試 ---


def test_parse_twse_company_info() -> None:
    """測試解析 TWSE 公司資料為 DataFrame。"""
    result = company_info.parse_twse_company_info(TWSE_SAMPLE_DATA)

    assert len(result) == 2
    assert list(result.columns) == [
        "SecurityCode", "IndustryCode", "CompanyName",
        "SpecialShares", "NormalShares", "PrivateShares",
    ]

    # 驗證第一筆（台泥）
    row0 = result.iloc[0]
    assert row0["SecurityCode"] == "1101"
    assert row0["IndustryCode"] == "01"
    assert row0["CompanyName"] == "臺灣水泥股份有限公司"
    assert row0["SpecialShares"] == 200000000
    assert row0["PrivateShares"] == 0
    # NormalShares = 77231817420 / 10 - 200000000 - 0
    assert row0["NormalShares"] == 7523181742

    # 驗證第二筆（台積電）
    row1 = result.iloc[1]
    assert row1["SecurityCode"] == "2330"
    assert row1["IndustryCode"] == "24"
    assert row1["SpecialShares"] == 0
    # NormalShares = 259303804580 / 10 - 0 - 0
    assert row1["NormalShares"] == 25930380458


def test_parse_tpex_company_info() -> None:
    """測試解析 TPEX 公司資料為 DataFrame。"""
    result = company_info.parse_tpex_company_info(TPEX_SAMPLE_DATA)

    assert len(result) == 2
    assert list(result.columns) == [
        "SecurityCode", "IndustryCode", "CompanyName",
        "SpecialShares", "NormalShares", "PrivateShares",
    ]

    # 驗證第一筆（環球晶）
    row0 = result.iloc[0]
    assert row0["SecurityCode"] == "6488"
    assert row0["IndustryCode"] == "24"
    assert row0["CompanyName"] == "環球晶圓股份有限公司"
    assert row0["SpecialShares"] == 0
    assert row0["PrivateShares"] == 0
    # NormalShares = 4356360380 / 10 - 0 - 0
    assert row0["NormalShares"] == 435636038


def test_parse_twse_empty_data() -> None:
    """測試解析空的 TWSE 資料。"""
    result = company_info.parse_twse_company_info([])
    assert len(result) == 0


def test_parse_tpex_empty_data() -> None:
    """測試解析空的 TPEX 資料。"""
    result = company_info.parse_tpex_company_info([])
    assert len(result) == 0


# --- build_industry_map 測試 ---


def test_build_industry_map() -> None:
    """測試建立產業對照表。"""
    twse_df = pd.DataFrame({
        "SecurityCode": ["1101", "2330"],
        "IndustryCode": ["01", "24"],
        "CompanyName": ["台泥", "台積電"],
        "SpecialShares": [0, 0],
        "NormalShares": [100, 200],
        "PrivateShares": [0, 0],
    })
    tpex_df = pd.DataFrame({
        "SecurityCode": ["6488"],
        "IndustryCode": ["24"],
        "CompanyName": ["環球晶"],
        "SpecialShares": [0],
        "NormalShares": [100],
        "PrivateShares": [0],
    })

    result = company_info.build_industry_map(twse_df, tpex_df)

    # TWSE: 01, 24; TPEX: 24
    assert len(result) == 3
    twse_entries = [r for r in result if r["Market"] == "TWSE"]
    tpex_entries = [r for r in result if r["Market"] == "TPEX"]
    assert len(twse_entries) == 2
    assert len(tpex_entries) == 1

    # 驗證 TWSE 產業名稱
    cement = next(r for r in twse_entries if r["IndustryCode"] == "01")
    assert cement["Industry"] == "水泥工業"

    semi = next(r for r in twse_entries if r["IndustryCode"] == "24")
    assert semi["Industry"] == "半導體業"

    # 驗證 TPEX 產業名稱
    assert tpex_entries[0]["Industry"] == "半導體業"


def test_build_industry_map_unknown_code() -> None:
    """測試未知產業代碼時回傳 '未知產業'。"""
    twse_df = pd.DataFrame({
        "SecurityCode": ["9999"],
        "IndustryCode": ["99"],
        "CompanyName": ["測試公司"],
        "SpecialShares": [0],
        "NormalShares": [100],
        "PrivateShares": [0],
    })
    tpex_df = pd.DataFrame(columns=[
        "SecurityCode", "IndustryCode", "CompanyName",
        "SpecialShares", "NormalShares", "PrivateShares",
    ])

    result = company_info.build_industry_map(twse_df, tpex_df)

    assert len(result) == 1
    assert result[0]["Industry"] == "未知產業"


# --- company_info_crawler 整合測試 ---


def test_company_info_crawler(mocker: MockerFixture) -> None:
    """測試 company_info_crawler 整合流程。"""
    mocker.patch(
        "tw_crawler.company_info.fetch_twse_company_info",
        return_value=TWSE_SAMPLE_DATA,
    )
    mocker.patch(
        "tw_crawler.company_info.fetch_tpex_company_info",
        return_value=TPEX_SAMPLE_DATA,
    )

    result = company_info.company_info_crawler()

    assert "company_info" in result
    assert "industry_map" in result
    assert "twse_count" in result
    assert "tpex_count" in result
    assert result["twse_count"] == 2
    assert result["tpex_count"] == 2
    assert len(result["company_info"]) == 4
    assert len(result["industry_map"]) > 0

    # 驗證合併後的資料包含 TWSE 與 TPEX
    codes = [r["SecurityCode"] for r in result["company_info"]]
    assert "1101" in codes  # TWSE
    assert "2330" in codes  # TWSE
    assert "6488" in codes  # TPEX
    assert "3105" in codes  # TPEX


def test_company_info_crawler_twse_failure(
    mocker: MockerFixture,
) -> None:
    """測試 TWSE API 失敗時拋出例外。"""
    mocker.patch(
        "tw_crawler.company_info.fetch_twse_company_info",
        side_effect=Exception("TWSE API error"),
    )

    try:
        company_info.company_info_crawler()
        assert False, "Should have raised an exception"
    except Exception as e:
        assert "TWSE API error" in str(e)
