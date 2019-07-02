import requests
import redis
import redlock
import logging
import json

import time
import os

class NPMDLProducer(object):

    redis = None
    base_uri = None
    logger = None
    redlock = None
    sequence = 0

    def __init__(self, uri='https://skimdb.npmjs.com/registry/'):
        self.base_uri = uri
        self.logger = logging.getLogger('producer')
        self.redis = redis.Redis('localhost', 6379)
        self.redlock = redlock.Redlock([{"host": "localhost", "port": 6379, "db": 0}, ])
        self.logger.info("producer started")


    def work(self):
        queueExhausted = None
        while queueExhausted != False:
            queueExhausted = self.getFromQueue()
            time.sleep(1)


    def getFromQueue(self):
        seq = self.redis.get("sequence")
        if seq == None:
            self.sequence = 0
            lock = self._get_sequence_lock()
            self.redis.set("sequence", int(self.sequence))
            self._remove_sequence_lock(lock)
        else:
            self.sequence = int(seq)

        target_uri = "https://skimdb.npmjs.com/registry/_all_docs?limit=200&include_docs=true&skip=%d" % self.sequence
        self.logger.info("← %s" % target_uri)
        resp = requests.get(target_uri)
        try:
            try:
                results = resp.json()
            except:
                return
            rows = results['rows']
            if len(rows) == 0:
                return False
            for row in rows:
                self.sequence += 1
                try:
                    doc = row['doc']
                    version = list(doc['versions'].values())[-1]
                    tarball = version['dist']['tarball']
                    filename = os.path.basename(tarball)
                    self.logger.info("↗ %s" % filename)
                    jsondata = json.dumps({ "uri": tarball, "retry": 0 })
                    self.redis.rpush("download", jsondata)
                except:
                    continue
        except:
            return
        finally:
            # update sequence
            lock = self._get_sequence_lock()
            self.redis.set("sequence", int(self.sequence))
            self._remove_sequence_lock(lock)


    def get_doc_count(self):
        status = requests.get('https://skimdb.npmjs.com/')
        count = int(status.json()['doc_count'])
        return count


    def _get_sequence_lock(self):
        mylock = False
        while mylock is False:
            mylock = self.redlock.lock("updateSequence", 1000)
            time.sleep(0.05)
        return mylock


    def _remove_sequence_lock(self, lock):
        self.redlock.unlock(lock)

