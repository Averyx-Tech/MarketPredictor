### utils
import shutil
import os
import logging
import sys
from pprint import pprint

### sklearn
from textblob import TextBlob
from sklearn.metrics import classification_report

### transformers
from transformers import AutoModelForSequenceClassification
import nltk

### finbert
from finbert.finbert import *
import finbert.utils as tools

###
import json
import pandas as pd
import itertools
###
import datetime
from dateutil.parser import parse
import time
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
### BERT_NER
from bert import Ner
### custom modules
from ner_utils import *

#model = Ner("./large/out_large/")

with open('./config.json') as f:
    config = json.load(f)

### mongodb connection
mg_host = config['mongodb']['host']
#mg_client = MongoClient(host=mg_host)

### error log db & collection 
error_log_db = config['bert_ner']['error_log_db']
error_log_collection = config['bert_ner']['error_log_collection']
result_db = config['bert_ner']['result_db']
result_collection = config['bert_ner']['result_collection']

### kafka connection info
bootstrap_servers = config['kafka']['bootstrap_servers']
consumer_topic = config['bert_ner']['consumer_topic']
group_id = config['bert_ner']['group_id']
threads = config['bert_ner']['threads']
producer_topic = config['bert_ner']['producer_topic']

### generating consumer objects
def get_consumer(host, topic, group_id):
    consumer = KafkaConsumer(bootstrap_servers=host, group_id=group_id)
    consumer.subscribe([topic])
    return consumer

consumers = {}
ner_models = {}
sentiment_models = {}
mg_clients = {}
for i in range(threads):
    consumers[i] = get_consumer(host=bootstrap_servers, topic=consumer_topic, group_id=group_id)
    mg_clients[i] = MongoClient(host=mg_host)
    ner_models[i] = Ner(".large/out_large/")
    sentiment_models[i] = AutoModelForSequenceClassification.from_pretrained("./finbert/finbert")

### bert_ner & bert_sentiment 
def bert_ner_sentiment(consumer, ner_model, sentiment_model, mg_client):
    for msg in consumer:
        record = json.loads(msg.value)
        text = record['title']+' '+record['summary']
        ner_out = []
        for chunks in text.split('.'):
            ner_out.append(ner_model.predict(chunks))
        ner_out = list(itertools.chain.from_iterable(ner_out))
        entities, flags = get_tags(ner_out)

        sentiment_out = predict(text, sentiment_model)
        sentiment_score = sentiment_out['sentiment_score'].to_list()
        sentiment_score = np.mean(sentiment_score)

        record.update(entities)
        record.update(flags)
        record['sentiment_score'] = sentiment_score

        mg_client[result_db][result_collection].insert(record)

### starting threads
jobs = {}
for i in range(threads):
    jobs[i] = threading.Thread(target=bert_ner_sentiment, args=(consumers[i], ner_models[i], sentiment_models[i], mg_clients[i], ))
    jobs[i].start()
    print("Thread: "+str(i)+" Started")
    
for i in range(threads):
    jobs[i].join()
