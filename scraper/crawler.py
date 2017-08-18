from datetime import datetime

import settings
from models import ProductRecord
from helpers import make_request, log, format_url, enqueue_url, dequeue_url
from extractors import get_title, get_url, get_price, get_primary_img, get_indexing
from amazon_api import amazon_api, amazon_product
from time import sleep, time
import multiprocessing as mp

crawl_time = datetime.now()

def begin_crawl(asin, keyword, country_host, country_code, output):
    returnDictionary = {}
    page, html = make_request(asin=asin, host=country_host, keyword=keyword)
    if page == None:
        log("WARNING: Error in {} found in the extraction.".format(asin))
        product_indexing = amazon_product(asin, keyword, country_code)
        returnDictionary[keyword] = product_indexing
    else:    
        item = page
        product_indexing = get_indexing(item)
        returnDictionary[keyword] = product_indexing
    output.put(returnDictionary)

def parallel_crawl(asin, keywords, country_host, country_code):
    # Define an output queue
    output_queue = mp.Queue()
    # Setup a list of processes that we want to run
    processes = [mp.Process(target=begin_crawl, args=(asin, keyword, country_host, country_code, output_queue)) for keyword in keywords]
    #intial_time = time()
    # Run processes
    for p in processes:
        p.start()

    # Exit the completed processes
    for p in processes:
        p.join()
    # Get process results from the output queue
    results = [output_queue.get() for p in processes]
    #final_time = time()
    return dict(pair for d in results for pair in d.items())

