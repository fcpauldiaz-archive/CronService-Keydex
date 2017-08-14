import sys
from datetime import datetime

import eventlet

import settings
from models import ProductRecord
from helpers import make_request, log, format_url, enqueue_url, dequeue_url
from extractors import get_title, get_url, get_price, get_primary_img, get_indexing
from amazon_api import amazon_api, amazon_product
from time import sleep

crawl_time = datetime.now()

pool = eventlet.GreenPool(settings.max_threads)
pile = eventlet.GreenPile(pool)


def begin_crawl(asin, keywords, country_host, country_code):
    returnDictionary = {}
    for keyword in keywords:
        print keyword
        page, html = make_request(asin=asin, host=country_host, keyword=keyword)
        if page == None:
            log("WARNING: Error in {} found in the extraction.".format(asin))
            sleep(2)
            product_indexing = amazon_product(asin, keyword, country_code)
            returnDictionary[keyword] = product_indexing
        else:    
            item = page
            product_indexing = get_indexing(item)
            returnDictionary[keyword] = product_indexing
    return returnDictionary


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
        sleep(3)
        pile.spawn(fetch_listing, ASIN, marketplace)
    if page == None:
        return None
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
