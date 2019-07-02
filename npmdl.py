from npmdl_producer import NPMDLProducer
from npmdl_worker import NPMDLWorker

import threading
import os
import sys
import logging

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

def worker_thread(download_dir):
    worker = NPMDLWorker(download_dir)
    worker.work()

def producer_thread():
    producer = NPMDLProducer()
    producer.work()

def main(num_producers, num_workers, location):
    workers = []
    producers = []

    for _ in range(num_producers):
        t = threading.Thread(target=producer_thread, args=())
        t.start()
        producers.append(t)

    for _ in range(num_workers):
        t = threading.Thread(target=worker_thread, args=(location, ))
        t.start()
        workers.append(t)

    logging.info("npmdl: spawned %d producers, %d workers" % (len(producers), len(workers)))

    

if __name__ == "__main__":
    main(1, 10, os.path.abspath('./packages'))