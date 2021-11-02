### scrapers
import feedparser
from bs4 import BeautifulSoup as bs
import requests
from werkzeug.urls import url_fix
from urllib.parse import unquote
### utils
from datetime import datetime
from time import mktime
import pandas as pd
import json
#mongodb
from pymongo import MongoClient

### project configuration
with open('../config.json') as f:
    config = json.load(f)

### mongodb connection
mg_host = config['mongodb']['host']
mg_port = config['mongodb']['port']
mg_client = MongoClient(host=mg_host, port=mg_port)

### searx search results
search_results = json.load(open('./all_countries_google_results_rss_newsfeed_sites.json', encoding='ISO-8859-1'))


def scrap_rss_links(url, country, mg_client_):
    try:
        payload = "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\nABC\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"

        headers = {
            'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
            'cache-control': "no-cache",
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
        }
        response = requests.request("GET", unquote(url), headers=headers, data=payload)
        et = bs(response.text, 'html.parser')
        all_links = et.find_all('a')
        result = et.find_all('rss')
        if not result:
            url_list = []
            rss = 0
            for a in all_links:
                if 'href' in a.attrs:
                    try:
                        response = requests.request("GET", a.attrs['href'], headers=headers, data=payload)
                    except Exception as e:
                        vendor = urlparse(url).netloc
                        vendor = re.sub(r'www.|WWW.','', vendor)
                        mg_client_.vigil.rss_links_new_error_logs.insert_one({"country": country, "vendor": vendor, 'Exception': str(e), "url": a.attrs['href'], "error_type": "link"})
                        print(e)
                        continue
                    et = bs(response.text, 'html.parser')
                    result = et.find_all('rss')
                    if result:
                        url_ = a.attrs['href']
                        vendor = urlparse(url).netloc
                        vendor = re.sub(r'www.|WWW.','', vendor)

                        last_poll =  '2021 06 01 00:00:00'
                        last_poll = datetime.strptime(last_poll, '%Y %m %d %H:%M:%S')

                        record = {
                            'country': country,
                            'vendor': vendor,
                            'url': url_,
                            'last_poll': last_poll,
                            'last_update': last_poll,
                            'pattern': []
                        }
                        print(record)
                        url_list.append(record)
            mg_client_.vigil.rss_links_new.insert_many(url_list)
        else:
            vendor = urlparse(url).netloc
            vendor = re.sub(r'www.|WWW.','', vendor)
            last_poll =  '2021 06 01 00:00:00'
            last_poll = datetime.strptime(last_poll, '%Y %m %d %H:%M:%S')
            
            record = {
                'country': country,
                'vendor': vendor,
                'url': url,
                'last_poll': last_poll,
                'last_update': last_poll,
                'pattern': []
            }
            mg_client_.vigil.rss_links_new.insert_one(record)
            print(record)
            
    except Exception as e:
        vendor = urlparse(url).netloc
        vendor = re.sub(r'www.|WWW.','', vendor)
        mg_client_.vigil.rss_links_new_error_logs.insert_one({"country": country, "vendor": vendor, 'exception': str(e), "url": url, "error_type": "unknown"})
        print(e)