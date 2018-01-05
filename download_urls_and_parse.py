#!/usr/bin/python


import json


from elasticsearch import Elasticsearch
es = Elasticsearch(http_auth=('elastic', 'secret'))

import threading
import time
import logging
import random
import Queue
import os
import hashlib
import tempfile

import itertools

from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium import webdriver  
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class screenshotPersister():
    def __init__(self):
        pass
    @property
    def save(self, fileid, contents):
        raise NotImplementedError("Subclasses should implement this!")

    @property
    def fetch(self, fileid):
        raise NotImplementedError("Subclasses should implement this!")

class fileScreenshotPersister(screenshotPersister):
    _dir_path = None

    def __init__(self, dir_path):
        self._dir_path = dir_path

    def save(self, fileid, contents):
        with open("{}/{}.png".format(self._dir_path, fileid), "w+") as f:
            f.write(contents)
            f.close()

    def fetch(self, fileid):
        ret = None
        with open("{}/{}.png".format(self._dir_path, fileid)) as f:
            ret= f.read()
        return ret
    


logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-9s) %(message)s',)

logging.getLogger('elasticsearch').setLevel(logging.ERROR)

BUF_SIZE = 10
MAX_DOWNLOAD_THREADS=2
CHROME_WINDOW_SIZE="800,600" # "1920,1080"

q = Queue.Queue(BUF_SIZE)

class elastic_reader(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(elastic_reader,self).__init__()
        self.target = target
        self.name = name

    def run(self):
        while True:
            if q.empty():
                all_items = es.search(index="urls", scroll = '2m', size = BUF_SIZE, body={"query": {"match_all": {}}})
                sid = all_items['_scroll_id']
                scroll_size = all_items['hits']['total']
                while (scroll_size > 0):
                    items = es.scroll(scroll_id = sid, scroll = '2m')['hits']['hits']

                    for item in items:
                        item = item["_source"]
                        q.put(item)
                        logging.debug('Putting ' + str(item['url']) + ' : ' + str(q.qsize()) + ' items in queue')
            time.sleep(1)

        return

class downloader(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(downloader,self).__init__()
        self.target = target
        self.name = name

        self.screenshotPersister = fileScreenshotPersister("/root/somethingphishy/screenshots")
        chrome_options = Options()
        #chrome_options.add_argument("--headless")
        #chrome_options.add_argument("--timeout=5000")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--window-size=%s" % CHROME_WINDOW_SIZE)


        self.driver = webdriver.Chrome(chrome_options=chrome_options, service_args=["--verbose", "--log-path=qc1.log"])
        # tested to see if it helps... it doesself
        #self.driver.set_page_load_timeout(10)

        return

    def run(self):
        while True:
            if not q.empty():
                item = q.get()
                p = str(item['url'])
                #p = "http://estrenospy.net/descargar-torrent-un-italiano-en-noruega-dvdrip-espanol-espana-por-putlocker-159526.fx"
                h = hashlib.sha256(p).hexdigest()

                logging.debug('Getting ' + str(p) 
                              + ' : ' + str(q.qsize()) + ' items in queue')
                logging.debug('Url SHA256: ' + str(h))
                fd, path = tempfile.mkstemp()
                logging.debug("Got temporary file: {}".format(path))
                
                try:
                    self.driver.get(p)
                    logging.debug("Fetched the url: {}".format(p))
                except WebDriverException:
                    logging.debug("Download interrupted: {}".format(item['url']))
                except:
                    raise
                
                #try:
                #    self.driver.save_screenshot(path)
                #    with open(path) as f:#    fd, path = tempfile.mkstemp()
                #        logging.debug("Saving screenshot")#    logging.debug("Got temporary file: {}".format(path))
                #        self.screenshotPersister.save(hashlib.sha256(p).hexdigest(), f.read())#    self.driver.get(p)
                #finally:
                #    s.remove(path)
                logging.debug("Saving screenshot")#
                self.driver.save_screenshot(path)#self.screenshotPersister.save(hashlib.sha256(p).hexdigest(), self.driver.get_screenshot_as_png())
        return

if __name__ == '__main__':
    downs=[]
    p = elastic_reader(name='producer')
    p.daemon = True
    p.start()
    for downloadern in xrange(MAX_DOWNLOAD_THREADS):
        c = downloader(name='downloader_{}'.format(downloadern))
        c.daemon = True
        c.start()
        downs+=[c]

    while True:
        time.sleep(11)
