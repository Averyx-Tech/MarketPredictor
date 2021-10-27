### scrapers
import feedparser
from bs4 import BeautifulSoup as bs
import requests
### utils
from datetime import datetime
from time import mktime

payload={}
headers={}
response = requests.request("GET", url, headers=headers, data=payload)
et = bs(response.text, 'html.parser')
all_links = et.find_all('a')
feed_list = []
rss = 0
for a in all_links:
    if 'href' in a.attrs:
        try:
            response = requests.request("GET", a.attrs['href'], headers=headers, data=payload)
        except:
            continue
        et = bs(response.text, 'html.parser')
        result = et.find_all('rss')
        if result:
            rss += 1
            print('found_rss :'+str(rss))
            print(a.attrs['href'])
            feed_list.append(a.attrs['href'])

       