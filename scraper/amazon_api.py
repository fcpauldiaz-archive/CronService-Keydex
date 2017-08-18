from amazon.api import AmazonAPI
from models import ProductRecord
from datetime import datetime
import settings

#function to retrieve a product information
#on a given marketplace
def amazon_api(asin, url, marketplace = 'US'):
  try:
    amazon = AmazonAPI(settings.AWS_KEY, settings.AWS_SECRET, settings.AWS_API, region=marketplace)
    product = amazon.lookup(ItemId=asin)
    model_product = ProductRecord(
      title=product.title,
      product_url='<missing product url>',
      listing_url=url,
      price=str(product.price_and_currency[1])+str(product.price_and_currency[0]),
      primary_img=product.large_image_url,
      product_indexing=None,
      crawl_time=datetime.now(),
      asin=asin.upper()
    )
    return model_product
  except:
    return None
  

#function to retrieve if a product is indexing
#on a given marketplace with a keyword
def amazon_product(asin, keyword, marketplace = 'US'):  
  try:
    amazon = AmazonAPI(settings.AWS_KEY, settings.AWS_SECRET, settings.AWS_API, region=marketplace)
    search_item = asin + ' ' + keyword
    products = amazon.search_n(1, Keywords=search_item, SearchIndex='All')
    if len(products) != 0:
      return True
    return False
  except:
    return False