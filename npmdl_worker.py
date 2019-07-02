import requests
import redis
import logging

import json
import time
import os

class NPMDLWorker(object):

    redis = None
    logger = None

    abspath = None

    def __init__(self, path=None):
        if not os.path.isdir(path):
            raise IOError("path not set")
        self.abspath = os.path.abspath(path)
        self.redis = redis.Redis('localhost', 6379)
        self.logger = logging.getLogger("worker")
        self.logger.info("worker started")


    def work(self):
        jsondata = ''
        while True:
            jsondata = self.redis.rpop("download")
            if jsondata != None:
                self.processWorkItem(json.loads(jsondata))
            else:
                time.sleep(0.1)


    def processWorkItem(self, jsondata):
        uri = jsondata['uri']
        retries = jsondata['retry']
        filename = os.path.basename(uri)

        download_location = os.path.join(self.abspath, filename)
        # don't download something we already have.
        if os.path.isfile(download_location):
            return

        try:
            self._download_file(uri, download_location)
            self.logger.info("↓ %s" % filename)
        except:
            if retries > 5:
                self.logger.info("✕ Too many errors: %s" % filename)
                return
            else:
                retries += 1
                self.logger.info("✕ Error %d: %s" % (retries, filename))

            # put back into work queue
            self.redis.rpush("download", json.dumps({ "uri": uri, "retry": retries }))

    
    def _download_file(self, uri, out):
        response = requests.get(uri, stream=True)
        handle = open(out, "wb")
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:  # filter out keep-alive new chunks
                handle.write(chunk)

        