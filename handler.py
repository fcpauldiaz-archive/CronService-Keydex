import json
import psycopg2
from scraper.crawler import begin_crawler, fetch_listing
import os

def hello(event, context):

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
    cur.execute('SELECT * FROM main_product')
    rows = cur.fetchall()
    print("The number of parts: ", cur.rowcount)
    asins = []
    for row in rows:
        asins.append(row[1])
    cur.close()
    conn.close()

    body = {
        "message": asins,
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY integration
    """
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
    """
