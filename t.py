import requests
from bs4 import BeautifulSoup
import threading
import re

txt = "yellow_tripdata_2010-07.parquet"
x = re.search(r"_tripdata_([0-9]{4})-([0-9]{2}).parquet$", txt)
print(True if x else False)
 
# url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
# reqs = requests.get(url)
# soup = BeautifulSoup(reqs.text, 'html.parser')
 
# urls = []
# links = [a for a in [b for b in soup.find_all('a') if b is not None] if a.get('href').endswith(".parquet")]
# for link in links: print(link.get('href'))
