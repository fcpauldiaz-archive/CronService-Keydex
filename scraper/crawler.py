import sys
from datetime import datetime

import eventlet

import settings
from models import ProductRecord
from helpers import make_request, log, format_url, enqueue_url, dequeue_url
from extractors import get_title, get_url, get_price, get_primary_img, get_indexing
from time import sleep

crawl_time = datetime.now()

pool = eventlet.GreenPool(settings.max_threads)
pile = eventlet.GreenPile(pool)


def begin_crawl(product):
    returnDictionary = {}
    baseUrl = "https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords="+product.asin
    for keyword in product.keywords:
        url = baseUrl+"+"+keyword
        page, html = make_request(url)
        if not page:
            log("WARNING: Error in {} found in the extraction.".format(url))
            return

        item = page
        product_indexing = get_indexing(item)
        returnDictionary[keyword] = product_indexing
        sleep(5)
        print "sleeping"
    return returnDictionary


def fetch_listing(ASIN):

    global crawl_time
    url = "https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords="+ASIN
    if not url:
        log("WARNING: No URLs {} found in the queue. Retrying...".format(url))
        #pile.spawn(fetch_listing)
        return

    page, html = make_request(url)
    if not page:
        return

    item = page
    product_image = get_primary_img(item)
    if not product_image:
        log("No product image detected, skipping")
        # continue
    product_title = get_title(item)
    product_url = get_url(item)
    product_price = get_price(item)
    product_indexing = get_indexing(item)


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
    #product_id = product.save()
    #download_image(product_image, product_id)

    # add next page to queue
    # next_link = page.find("a", id="pagnNextLink")
    # if next_link:
    #     log(" Found 'Next' link on {}: {}".format(url, next_link["href"]))
    #     enqueue_url(next_link["href"])
    #     pile.spawn(fetch_listing)


if __name__ == '__main__':

    if len(sys.argv) > 1 and sys.argv[1] == "start":
        log("Seeding the URL frontier with subcategory URLs")
        begin_crawl()  # put a bunch of subcategory URLs into the queue

    log("Beginning crawl at {}".format(crawl_time))
    [pile.spawn(fetch_listing) for _ in range(settings.max_threads)]
    pool.waitall()
