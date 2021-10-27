
import feedparser
from bs4 import BeautifulSoup as bs
import requests
###
import json
import pandas as pd
###
import datetime
from dateutil.parser import parse
from time import mktime
###
from rss_parser import *

with open('./rssfeed_links.json') as f:

  rss_list = json.load(f)

url = 'http://www.moneycontrol.com/rss/currency.xml'
url = 'https://www.novartis.com/rss/feeds/en.xml'
url = 'https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire - News about Public Companies'
pattern = ['summary']

#url = 'https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire - News about Public Companies'
#pattern = ['summary', 'p']

parser = RSS_Parser()

today = datetime.datetime.now(datetime.timezone.utc)
delta = datetime.timedelta(days=2)
latest_dt = today - delta

dt = parse('2021-05-02 00:00:00')
dt = dt.astimezone(datetime.timezone.utc)

news = parser.parse(url, '', dt, pattern)
print(url)
print(news)


# testfile = open("testfile.txt","w", encoding='utf-8') 
# for record in rss_list:
#     for url in record['rss_links']:
#         temp = {}
#         news = parser.parse(url, '', latest_dt, record['pattern'])
#         temp['news'] = news
#         temp['vendor'] = record['vendor']
#         temp['url'] = url
        
#         print(url)
#         testfile.write(str(temp)+',')
        

# testfile.close()