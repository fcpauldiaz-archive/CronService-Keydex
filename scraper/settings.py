import os

current_dir = os.path.dirname(os.path.realpath(__file__))

# Database
database = "amazon_crawler"
host = "localhost"
user = "postgres"

# Redis
redis_host = "localhost"
redis_port = 6379
redis_db = 0

# Request
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, sdch, br",
    "Accept-Language": "en-US,en;q=0.8",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
}
USER_AGENTS = [
  {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0'},
  {'User-Agent': 'Mozilla/5.0 (iPad; CPU OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1'},
  {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'},
  {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.111 Safari/537.36'},
  {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'},
  {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30'},
  {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'},
  {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0'},
  {'User-Agent': 'Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9.2.8) Gecko/20100723 Ubuntu/10.04 (lucid) Firefox/3.6.8'},
  {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)'}
]
allowed_params = ["node", "rh", "page"]

# Proxies
proxies = [
    # your list of proxy IP addresses goes here
    # check out https://proxybonanza.com/?aff_id=629
    # for a quick, easy-to-use proxy service
]
proxy_user = ""
proxy_pass = ""
proxy_port = ""

# Crawling Logic
start_file = os.path.join(current_dir, "start-urls.txt")
max_requests = 2 * 10**6  # two million
max_details_per_listing = 9999

# Threads
max_threads = 200

# Logging & Storage
log_stdout = True
image_dir = "/tmp/crawl_images"
export_dir = "/tmp"
