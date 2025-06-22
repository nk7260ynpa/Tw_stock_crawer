from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import random

options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
driver = webdriver.Chrome(options=options)

driver.get("https://tw.stock.yahoo.com/tw-market/")

prev_height = driver.execute_script("return document.body.scrollHeight")
for i in range(2):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2.32) 
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == prev_height:
        break
    prev_height = new_height

html = driver.page_source
driver.quit()

with open("output.html", "w", encoding="utf-8") as file:
    file.write(html)