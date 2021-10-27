"""
    Type: Job
    Developer: Vignesh
    Description: Parallel consumers scraping contents from URL and send it to kafka topic
"""

### scraper
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
### multithreading
import threading
### kafka
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import TopicPartition
### mongodb
from pymongo import MongoClient
from bson.objectid import ObjectId
### custom module
from rss_parser import *

with open('./config.json') as f:
    config = json.load(f)

### mongodb connection
mg_host = config['mongodb']['host']
mg_port = config['mongodb']['port']
mg_client = MongoClient(host=mg_host, port=mg_port)

### error log db & collection 
error_log_db = config['rss_scrapers']['error_log_db']
error_log_collection = config['rss_scrapers']['error_log_collection']

### rss_links db & collection
rss_links_db = config['rss_scrapers']['rss_links_db']
rss_links_collection = config['rss_scrapers']['rss_links_collection']

### kafka connection info
bootstrap_servers = config['kafka']['bootstrap_servers']
consumer_topic = config['rss_scrapers']['consumer_topic']
group_id = config['rss_scrapers']['group_id']
threads = config['rss_scrapers']['threads']
producer_topic = config['rss_scrapers']['producer_topic']

### kafka producer object
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

### generating consumer objects
def get_consumer(host, topic, group_id):
    consumer = KafkaConsumer(bootstrap_servers=host, group_id=group_id)
    consumer.subscribe([topic])
    return consumer

consumers = {}
parsers = {}
for i in range(threads):
    consumers[i] = get_consumer(host=bootstrap_servers, topic=consumer_topic, group_id=group_id)
    parsers[i] = RSS_Parser()

### parser function; thread function
def rss_parser(consumer, parser):
    for msg in consumer:
        record = json.loads(msg.value)
        last_update = record['last_update']
        last_update = parse(last_update)
        last_update = last_update.astimezone(datetime.timezone.utc)
        news_record, latest_update = parser.parse(record['url'], '', last_update, record['pattern'])
        if news_record:
            if news_record[0] == 'Reject':
                log = {
                    "type": "Reject",
                    "vendor": record['vendor'],
                    "url": record['url']
                }
                mg_client[error_log_db][error_log_collection].insert(log)

            elif news_record[0] == 'Exception':
                log = {
                    "type": "Exception",
                    "vendor": record['vendor'],
                    "url": record['url'],
                    "error_msg": news_record[1]
                }
                mg_client[error_log_db][error_log_collection].insert_one(log)

            else:
                for news in news_record:
                    print(news)
                    news['published'] = str(news['published'])
                    news['vendor'] = record['vendor']
                    producer.send(producer_topic, bytes(json.dumps(news), 'utf-8'))

                mg_client[rss_links_db][rss_links_collection].update_one({"_id": ObjectId(record["_id"])}, {"$set": {"last_update": str(latest_update)}})


### starting threads
jobs = {}
for i in range(threads):
    jobs[i] = threading.Thread(target=rss_parser, args=(consumers[i], parsers[i],))
    jobs[i].start()
    print("Thread: "+str(i)+" Started")
    
for i in range(threads):
    jobs[i].join()




