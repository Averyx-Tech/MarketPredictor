
### scrapers
import feedparser
from bs4 import BeautifulSoup as bs
import requests
from werkzeug.urls import url_fix
from urllib.parse import unquote, urlparse, quote_plus

### utils
from datetime import datetime
from time import mktime
import pandas as pd
import json
import regex as re

### mongo
from pymongo import MongoClient

### mutlithreading
import threading
from multiprocessing import *

### timeout
from func_timeout import func_timeout, FunctionTimedOut


search_results = json.load(open('../all_countries_google_results_rss_newsfeed_sites.json', encoding='ISO-8859-1'))


client = MongoClient(host="mongodb://{}:{}@ec2-18-116-108-20.us-east-2.compute.amazonaws.com".format(quote_plus("vignesh"), quote_plus("Vicky2201")))

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}
def get_requests(url):
    response = requests.request("GET", url, headers=headers, timeout=5)
    return response


def url_check(base_url, url):
    
    if len(url) > 1:
        if url[0] == '/':
            corrected_url = 'http://' + base_url + url
        elif  ('http://' not in url) and ('https://' not in url):
            corrected_url = 'http://' + base_url + '/' + url
        else:
            return url
    else:
        return url
    
    return corrected_url


def scrap_rss_links(url, country, mg_client_):
    try:
        
        vendor = urlparse(url).netloc
        vendor = re.sub(r'www.|WWW.','', vendor)
        
        last_poll =  '2021 06 01 00:00:00'
        last_poll = datetime.strptime(last_poll, '%Y %m %d %H:%M:%S')

        try:
            response = func_timeout(10, get_requests, args=([unquote(url)]))
        except FunctionTimedOut as e:
            return e
        except Exception as e:
            return e

        et = bs(response.text, 'html.parser')
        all_links = et.find_all('a')
        result = et.find_all('rss')
        
        if not result:
            url_list = []
            rss = 0
            for a in all_links:
                if 'href' in a.attrs:
                    fixed_url = url_check(vendor, a.attrs['href'])
                    try:
                        response = func_timeout(10, get_requests, args=([fixed_url]))
                    except FunctionTimedOut as e:
                        mg_client_.vigil.rss_links_new_error_logs.insert_one({"country": country, "vendor": vendor, 'Exception': str(e), "url": fixed_url, "error_type": "link"})
                        continue
                    except Exception as e:
                        mg_client_.vigil.rss_links_new_error_logs.insert_one({"country": country, "vendor": vendor, 'Exception': str(e), "url": fixed_url, "error_type": "link"})
                        continue
            
                    et = bs(response.text, 'html.parser')
                    result = et.find_all('rss')
                    if result:

                        record = {
                            'country': country,
                            'vendor': vendor,
                            'url': fixed_url,
                            'last_poll': last_poll,
                            'last_update': last_poll,
                            'pattern': [],
                            "fixed_url": 1
                        }
                        mg_client_.vigil.rss_links_new.insert_one(record)
                        print(record)
                    else:
                        print("---- No rss on ---- "+fixed_url)
            
        else:        
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


def processSpawner(topic=None, links=[], client=None):
    
        start_time = str(datetime.now())
        for link in links:
            scrap_rss_links(link, topic, client)
        end_time = str(datetime.now())
        record = {
            "country": topic,
            "status": "completed",
            "start_time": start_time,
            "end_time": end_time
        }
        client.links_management.link_scraper_status.insert_one(record)
        client.close()


status = list( client.links_management.link_scraper_status.find({"status": "completed"}) )
status = pd.DataFrame(status)
scraped_countries = status[status["status"] == "completed"]['country'].to_list()

search_results = [ _ for _ in search_results if _['country'] not in scraped_countries ]

n = len(search_results)
threads = 4 
start_limit = 0

### starting threads
for end_limit in range(start_limit, n, threads):
    clients = []
    jobs = []
    idx = 0

    for record in search_results[start_limit: end_limit]:
        clients.append( MongoClient(host="mongodb://{}:{}@ec2-18-116-108-20.us-east-2.compute.amazonaws.com".format(quote_plus("vignesh"), quote_plus("Vicky2201"))) )    
        jobs.append( threading.Thread(target=processSpawner, args=(record['country'], record['links'], clients[idx], )) )
        jobs[idx].start()
        idx += 1
        print("Thread: "+str(idx)+" Started")

    for _ in range(idx):
        jobs[_].join()

    start_limit = end_limit

client.close()
