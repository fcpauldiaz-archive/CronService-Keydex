# -*- coding: utf-8 -*-
import json
import psycopg2
from scraper.crawler import begin_crawl
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
import requests
import os

def get_user_data(product_id, cursor):
    query = '''
        SELECT DISTINCT u.first_name, u.last_name, u.email
        FROM auth_user u
        INNER JOIN main_product p on p.user_id = u.id
        WHERE p.id = %s;
    '''
    cursor.execute(query, str(product_id))
    rows = cursor.fetchone()
    if rows != None:
        return rows[0], rows[1], rows[2]
    return None


def send_email(asin, first_name, last_name, keywords, rate, email_to):
    env = Environment(loader=FileSystemLoader(os.path.dirname(os.path.abspath(__file__))), trim_blocks=True)
    template = env.get_template('templates/email_reporting.html')
    html = template.render(
        asin=asin, 
        first_name=first_name, 
        last_name=last_name,
        keywords=keywords,
        rate=rate
    )
    email_format = first_name + ' ' + last_name + '<' + email_to + '>'
    requests.post(
        'https://api.mailgun.net/v3/mail.checkmykeywords.com/messages',
        auth=('api', 'key-65265adea3b2c11f6282b435df3c7505'),
        #files=[("inline", open("./foto2.png"))],
        data={
          'from': 'Check My Keywords <do-not-reply@mail.checkmykeywords.com>',
          'to': [email_format],
          'subject': "Your Keyword's Report",
          'text': 'https://dev.checkmykeywords.com/dashboard',
          'html': html
        }
    )



def save_product_indexing(result, product_id, cursor, conn):
  indexed = 0.0
  indexing_data = {}
  keyword_length = 0
  for keyword, indexing in result.items():
    query = '''
        INSERT INTO public.main_keywords
        (id, keyword, indexing, index_date, product_id)
        VALUES(nextval('main_keywords_id_seq'::regclass), %s, %s, %s, %s);
    '''
    data = (keyword, indexing, datetime.now(), product_id)
    cursor.execute(query, data)
    conn.commit()

    if (indexing == True):
      indexed += 1
    keyword_length += 1

  indexing_rate = float(indexed)/float(keyword_length) * 100
  query = '''
    INSERT INTO public.main_product_historic_indexing
    (id, indexing_rate, indexed_date, product_id)
    VALUES(nextval('main_product_historic_indexing_id_seq'::regclass), %s, %s, %s);
  '''
  data = (indexing_rate, datetime.now(), product_id)
  #save transactional operation
  cursor.execute(query, data)
  conn.commit()
  return indexing_rate


def hello():

    # Connect to an existing database
    if 'DB_HOST' in os.environ:
      conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        host=os.environ['DB_HOST'],
        port=5432
      )
    else:
      conn = psycopg2.connect(
        dbname='keydex',
        user='',
        password='',
        host='localhost',
        port=5432
      )


    cur = conn.cursor()
    cur.execute('''
      SELECT p.id, p.asin, p.keywords, p.reporting_percentage, r.periodicity, mrk.country_code, mrk.country_host
        FROM main_product p
        INNER JOIN main_reporting_period r on p.reporting_period_id = r.id
        INNER JOIN main_marketplace mrk on p.marketplace_id = mrk.id;
    ''')
    rows = cur.fetchall()
    #asin, keywords, reporting_percentage, periodicity
    

    for row in rows:
        try:
            product_id = row[0]
            asin =  row[1]
            keywords =  row[2]
            reporting_percentage = row[3]
            periodicity = row[4]
            country_code = row[5]
            country_host = row[6]
            rDict = begin_crawl(asin, keywords, country_host, country_code)
            rate = save_product_indexing(rDict, product_id, cur, conn)
            
            if reporting_percentage >= 100:
                first_name, last_name, email_to = get_user_data(product_id, cur)
                #send email
                send_email(asin, first_name, last_name, keywords, rate, email_to)
            elif reporting_percentage >= rate:
                first_name, last_name, email_to = get_user_data(product_id, cur)
                #send email
                send_email(asin, first_name, last_name, keywords, rate, email_to)
        except:
            #continue for next product
            pass
        
    cur.close()
    conn.close()

    # body = {
    #     "message": asins,
    #     "input": event
    # }

    # response = {
    #     "statusCode": 200,
    #     "body": json.dumps(body)
    # }

    # return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY integration
    """
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
    """
hello()