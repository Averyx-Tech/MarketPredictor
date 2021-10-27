"""
    Type: Job
    Developer: Vignesh
    Description: RSS link to kafka topic to get utilized by RSS_Scrapers
"""


### utils
import json
import pandas as pd
### kafka
from kafka import KafkaProducer
### mongodb
from pymongo import MongoClient

with open('./config.json') as f:
    config = json.load(f)

### mongodb connection
mg_host = config['mongodb']['host']
mg_client = MongoClient(host=mg_host)

### kafka connection
bootstrap_servers = config['kafka']['bootstrap_servers']
topic = config['rss_link_producer']['producer_topic']
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

### fetching rss_links
mg_db = config['rss_link_producer']['database']
mg_collection = config['rss_link_producer']['collection']
rss_links = mg_client[mg_db][mg_collection].find({})

rss_links = list(rss_links)
print("topic: "+topic)

for record in rss_links:
    record['_id'] = str(record['_id'])
    record['last_poll'] = str(record["last_poll"])
    record['last_update'] = str(record["last_update"])
    producer.send(topic, bytes(json.dumps(record), 'utf-8'))

print('Produced all Links!')