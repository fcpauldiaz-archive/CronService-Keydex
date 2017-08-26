# -*- coding: utf-8 -*-
import os
import random
from datetime import datetime
from urlparse import urlparse
import requests

from bs4 import BeautifulSoup
from requests.exceptions import RequestException

import settings

num_requests = 0

def get_proxy():
    # choose a proxy server to use for this request, if we need one
    return {
        "http": "83.149.70.159:13012",
        "https": "83.149.70.159:13012"
    }


def make_request(asin, host, keyword=None, return_soup=True):
    # global request building and response handling

    #url = format_url(url)

    global num_requests
    if num_requests >= settings.max_requests:
        raise Exception("Reached the max number of requests: {}".format(settings.max_requests))

    #proxies = get_proxy()
    try:
        url = host+"/s/ref=nb_sb_noss"
        params = asin
        if keyword != None:
            params += " " + keyword.strip()
        querystring = {"url":"search-alias=aps", "field-keywords": params }

        headers = {
            'cache-control': "no-cache",
            'user-agent': random.choice(settings.USER_AGENTS)['User-Agent']
        }
        r = requests.request("GET", url, headers=headers, params=querystring, proxies=get_proxy())
        print r.url
    except RequestException as e:
        log("WARNING: Request for {} {} failed, trying again.".format(url, querystring))
        log(e.message)
        return None, None
        #return make_request(url)  # try request again, recursively

    num_requests += 1

    if r.status_code != 200:
        os.system('say "Got non-200 Response"')
        log("WARNING: Got a {} status code for URL: {}".format(r.status_code, url))
        return None, None

    if return_soup:
        return BeautifulSoup(r.content, 'lxml'), r.content
    return r


def format_url(url):
    # make sure URLs aren't relative, and strip unnecssary query args
    u = urlparse(url)

    scheme = u.scheme or "https"
    host = u.netloc or "www.amazon.com"
    path = u.path
    if not u.query:
        query = ""
    else:
        query = '?'+u.query
        # for piece in u.query.split("&"):
        #     k, v = piece.split("=")
        #     if k in settings.allowed_params:
        #         query += "{k}={v}&".format(**locals())
        # query = query[:-1]

    return "{scheme}://{host}{path}{query}".format(**locals())


def log(msg):
    # global logging function
    if settings.log_stdout:
        try:
            print "{}: {}".format(datetime.now(), msg)
        except UnicodeEncodeError:
            pass  # squash logging errors in case of non-ascii text




def enqueue_url(u):
    url = format_url(u)
    return redis.sadd("listing_url_queue", url)


def dequeue_url():
    return redis.spop("listing_url_queue")

def chooseHeader(header):
    header['User-Agent'] = random.choice(settings.USER_AGENTS)['User-Agent']
    return header


if __name__ == '__main__':
    # test proxy server IP masking
    r = make_request('https://api.ipify.org?format=json', return_soup=False)
    print r.text