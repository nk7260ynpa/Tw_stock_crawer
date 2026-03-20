"""台灣股市爬蟲套件。

提供上市(TWSE)、上櫃(TPEX)、期貨(TAIFEX)、三大法人(FAOI)、融資融券(MGTS)、
集保戶股權分散表(TDCC)、工商時報新聞(CTEE)、鉅亨網新聞(CNYES)、
PTT股版新聞(PTT)、聯合新聞網經濟日報新聞(MoneyUDN)、
公司產業對照(CompanyInfo)、國際原油價格(OilPrice)、
國際黃金價格(GoldPrice)、比特幣價格(BitcoinPrice)、
國際匯率(CurrencyPrice)、國際股市指數(IndicesPrice) 的爬蟲函式。
"""

from .twse import twse_crawler
from .tpex import tpex_crawler
from .taifex import taifex_crawler
from .faoi import faoi_crawler
from .mgts import mgts_crawler
from .tdcc import tdcc_crawler
from .ctee_news import ctee_news_crawler
from .cnyes_news import cnyes_news_crawler
from .ptt_news import ptt_news_crawler
from .moneyudn_news import moneyudn_news_crawler
from .company_info import company_info_crawler
from .oil_price import oil_price_crawler
from .gold_price import gold_price_crawler
from .bitcoin_price import bitcoin_price_crawler
from .currency_price import currency_price_crawler
from .indices_price import indices_price_crawler