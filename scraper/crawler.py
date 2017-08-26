from datetime import datetime

import settings
from models import ProductRecord
from helpers import make_request, log, format_url, enqueue_url, dequeue_url
from extractors import get_title, get_url, get_price, get_primary_img, get_indexing
from amazon_api import amazon_api, amazon_product
from time import sleep, time
import multiprocessing as mp

crawl_time = datetime.now()

def begin_crawl(asin, keyword, country_host, country_code, retries, output):
    returnDictionary = {}
    page, html = make_request(asin=asin, host=country_host, keyword=keyword)
    if page == None:
        log("WARNING: Error in {} found in the extraction. keyword {}".format(asin, keyword))
        sleep(2+retries)
        if (retries < 3):
            return begin_crawl(asin, keyword, country_host, country_code, retries+1, output)
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
    processes = [mp.Process(target=begin_crawl, args=(asin, keyword, country_host, country_code, 0, output_queue)) for keyword in keywords]
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

def fetch_listing(ASIN, marketplace):

    global crawl_time
    url = marketplace.country_host+"/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords="+ASIN
    if not url:
        log("WARNING: No URLs {} found in the queue. Retrying...".format(url))
        #pile.spawn(fetch_listing)
        return

    page, html = make_request(ASIN, marketplace.country_host)
    if not page:
        log("WARNING: No page. Retrying")
        #sleep(3)
        #fetch_listing(ASIN, marketplace)
    if page == None:
        return amazon_api(ASIN, url, marketplace.country_code)
    item = page
    product_image = get_primary_img(item)
    if not product_image:
        log("No product image detected, skipping")
        # continue
    product_title = get_title(item)
    product_url = get_url(item)
    product_price = get_price(item)
    product_indexing = get_indexing(item)
    if (product_title == '<missing product title>' and 
        product_url == '<missing product url>'):
        product = amazon_api(ASIN, url, marketplace.country_code)
    else:
        product = ProductRecord(
            title=product_title,
            product_url=format_url(product_url),
            listing_url=format_url(url),
            price=product_price,
            primary_img=product_image,
            product_indexing=product_indexing,
            crawl_time=crawl_time,
            asin=ASIN
        ) 
    return product
