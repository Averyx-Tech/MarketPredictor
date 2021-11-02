### utils
import shutil
import os
import logging
import sys
from pprint import pprint

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
### nltk
import nltk
from nltk.corpus import stopwords
stopwords = stopwords.words('english')


#model = Ner("./large/out_large/")

with open('../config.json') as f:
    config = json.load(f)
    
    
### mongodb connection
mg_host = config['mongodb']['host']
mg_client = MongoClient(host=mg_host)

db = config['bert_ner']['result_db']
collection = config['bert_ner']['result_collection']

cursor = mg_client[db][collection].find({})

ner = ['ORG_E', 'PER_E', 'LOC_E', 'MISC_E']
verb_tags = ['VB', 'VBD', 'VBG', 'VBN', 'VBZ']
removed_tags = ['VBP']

while (cursor.hasNext()) {
    record = next(cursor)
    
    entity_pos = []
    for ner_type in ner:
        for entity in record[ner_type]:
            pos = (record['title']+' '+record['summary']).index(entity)
            _ = {'entity': entity, 'pos': pos, 'ner_type': ner_type}
            entity_pos.append(_)
    entity_pos = sorted(entity_pos, key = lambda i: i['pos'])
    
}

