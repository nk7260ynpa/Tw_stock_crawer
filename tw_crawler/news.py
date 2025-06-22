import random
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time

options = Options()
options.add_argument("--headless")  # 不開 GUI
options.add_argument("--disable-gpu")
driver = webdriver.Chrome(options=options)

driver.get("https://tw.stock.yahoo.com/tw-market/")

prev_height = driver.execute_script("return document.body.scrollHeight")
for i in range(5):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(random.uniform(5, 15))  # 等候載入
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == prev_height:
        break
    prev_height = new_height

html = driver.page_source
driver.quit()

data = BeautifulSoup(html, "html.parser")
body_elements = data.find('body')
app_div = body_elements.find('div', id='app')
reactroot_div = app_div.find('div', attrs={'data-reactroot': True})
all_divs = reactroot_div.find('div')
target_div = reactroot_div.find('div', class_='render-target-active render-target-default', id='render-target-default')
div2 = target_div.find('div', class_='app app-rwd D(f) Fxd(c) Bxz(bb) Mih(100vh)')
target_div2 = div2.find('div', class_='W(100%) Bgc(#fff) Z(1)')
target_div3 = target_div2.find('div', class_='W(100%) Px(20px) Mx(a) Bxz(bb) Bgc(#fff) container D(f) Fxd(c) Fx(a) Z(1) Miw($w-container-min) Maw($w-container-max)')
target_div4 = target_div3.find('div', class_='D(f) Fx(a) Mb($m-module)')
layout_col1_div = target_div4.find('div', class_='Fxg(1) Fxs(1) Fxb(100%) W(0) Miw(0) Maw(900px)', id='layout-col1')
layout_col1_div2 = layout_col1_div.find('div')
ydc_stream_proxy = layout_col1_div2.find('div', id='YDC-Stream-Proxy')
ydc_stream_proxy2 = ydc_stream_proxy.find('div', class_='tdv2-applet-stream Bdc(#e2e2e6)', id='YDC-Stream', style='max-width:initial')
ul_element = ydc_stream_proxy2.find('ul', class_='My(0) P(0) Wow(bw) Ov(h)')
li_elements = ul_element.findAll('li', class_='js-stream-content Pos(r)')

news_head = []
for li_element in li_elements:
    div_element = li_element.find('div', class_='Py(14px) Pos(r)', attrs={'data-test-locator': 'mega'})
    cf_div = div_element.find('div', class_='Cf')
    target_div = cf_div.find('div', class_='Ov(h) Pend(14%) Pend(44px)--sm1024')
    if not target_div:
        continue  # 如果沒有找到目標div，則跳過這次循環
    target_div2 = target_div.find('h3', class_='Mt(0) Mb(8px)')
    u_element = target_div2.find('u', class_='StretchedBox')
    a_element = cf_div.find('a', class_='Fw(b) Fz(20px) Fz(16px)--mobile Lh(23px) Lh(1.38)--mobile C($c-primary-text)! C($c-active-text)!:h LineClamp(2,46px)!--mobile LineClamp(2,46px)!--sm1024 mega-item-header-link Td(n) C(#0078ff):h C(#000) LineClamp(2,46px) LineClamp(2,38px)--sm1024 not-isInStreamVideoEnabled')
    text = a_element.text

    target_div = cf_div.find('div', class_='Ov(h) Pend(14%) Pend(44px)--sm1024')
    nested_div = target_div.find('div', class_='C(#959595) Fz(13px) C($c-secondary-text)! D(ib) Mb(6px)')

    second_span = nested_div.find_all('span')[1]
    time_text = second_span.text
    today = datetime.today()
    if time_text == "昨天":
        date = today - timedelta(days=1)
    elif time_text == "前天":
        date = today - timedelta(days=2)
    else:
        date = today
    date = date.strftime('%Y-%m-%d')
    print(text, "   ", date)