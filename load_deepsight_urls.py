#!/usr/bin/env python
from __future__ import print_function

import logging
import pprint
import csv 
import io
import json
import hashlib
import config
import sqlite3

from configLoader import configLoader

config = configLoader.readConfiguration("load_deepsight_urls.conf.json")
conn = sqlite3.connect(config["database"]["filename"])

from datetime import datetime

from elasticsearch import Elasticsearch
es = Elasticsearch(http_auth=(config["elasticsearch"]["username"], config["elasticsearch"]["password"]))

from deepsight_feeds import DeepSightFeeds, FeedBaseException


# Name                                         ID
# ===================================================
# Advanced IP Reputation Attack CSV Feed       25
# Advanced IP Reputation Attack XML Feed       26
# Advanced IP Reputation Attack CEF Feed       27
# Advanced IP Reputation Bot CSV Feed          28
# Advanced IP Reputation Bot XML Feed          29
# Advanced IP Reputation Bot CEF Feed          30
# Advanced IP Reputation CnC CSV Feed          31
# Advanced IP Reputation CnC XML Feed          32
# Advanced IP Reputation CnC CEF Feed          33
# Advanced IP Reputation Fraud CSV Feed        34
# Advanced IP Reputation Fraud XML Feed        35
# Advanced IP Reputation Fraud CEF Feed        36
# Advanced IP Reputation Malware CSV Feed      37
# Advanced IP Reputation Malware XML Feed      38
# Advanced IP Reputation Malware CEF Feed      39
# Advanced IP Reputation Phishing CSV Feed     40
# Advanced IP Reputation Phishing XML Feed     41
# Advanced IP Reputation Phishing CEF Feed     42
# Advanced IP Reputation Spam CSV Feed         43
# Advanced IP Reputation Spam XML Feed         44
# Advanced IP Reputation Spam CEF Feed         45
# Advanced URL Reputation Attack CSV Feed      46
# Advanced URL Reputation Attack XML Feed      47
# Advanced URL Reputation Attack CEF Feed      48
# Advanced URL Reputation CnC CSV Feed         49
# Advanced URL Reputation CnC XML Feed         50
# Advanced URL Reputation CnC CEF Feed         51
# Advanced URL Reputation Fraud CSV Feed       52
# Advanced URL Reputation Fraud XML Feed       53
# Advanced URL Reputation Fraud CEF Feed       54
# Advanced URL Reputation Malware CSV Feed     55
# Advanced URL Reputation Malware XML Feed     56
# Advanced URL Reputation Malware CEF Feed     57
# Advanced URL Reputation Phishing CSV Feed    58
# Advanced URL Reputation Phishing XML Feed    59
# Advanced URL Reputation Phishing CEF Feed    60

interesting_feed_list = ['46'] #, '49', '52', '55', '58']

feed_list=[]

class DeepsightHelper:
    feeds = None
    logger = None

    def __init__(self, username, password):
        logger = logging.getLogger('')
        self.feeds = DeepSightFeeds(username=username, password=password)

    def getLatestFeedSeq(self, feed_id):
        logger.debug("Getting feeds for feed_id : {}".format(feed_id))
        response = list(self.feeds.getFeedFileList(feed_id))
        last = response[-1]
        return last[0]

    def getLatestFeedContent(self, feed_id):
	return self.getFeedContent(feed_id, self.getLatestFeedSeq(feed_id))
   
    def getFeedContent(self, feed_id, seq):
        name, date, data = self.feeds.getFeedFile(feed_id, seq)

        # convert to dict
	d = io.StringIO(unicode(data))
        reader_list = csv.DictReader(d)
        
        return reader_list

    def get_all_feeds(self):
        return feeds.getCustomerDataFeedList()
        

if __name__ == "__main__":
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    dh = DeepsightHelper(config["deepsight"]["username"], config["deepsight"]["password"])
    for feed_id in interesting_feed_list:
	logger.debug("Processing feed: {}".format(feed_id));
        for feed in dh.getLatestFeedContent(feed_id):
            timestamp = datetime.now().isoformat()
            feed['timestamp'] = timestamp
	    feed = dict((k, v) for k, v in feed.iteritems() if v)
            if 'url' not in feed or ('url' in feed and feed['url'] == ""):
		print (feed['domain_name'])
		index = "domains"
		url_id = ""
	    else:
		url_id = hashlib.sha224(feed['url']).hexdigest()
		index = "urls"
	    res = es.index(index=index, doc_type='url', id=url_id, body=json.dumps(feed) )
            


