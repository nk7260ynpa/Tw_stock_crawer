"""台灣股市爬蟲套件。

提供上市(TWSE)、上櫃(TPEX)、期貨(TAIFEX)、三大法人(FAOI)、融資融券(MGTS) 的爬蟲函式。
"""

from .twse import twse_crawler
from .tpex import tpex_crawler
from .taifex import taifex_crawler
from .faoi import faoi_crawler
from .mgts import mgts_crawler