import requests
import json 
import sqlalchemy as sa
import pandas as pd
from configparser import ConfigParser
import urllib.request

def get_num_page(url):
    response = urllib.request.urlopen(url, timeout = 10)
    num_page = json.loads(response.read())['paging']['last_page']
    return num_page

def get_data(engine):
    return pd.read_sql(
                        '''
                        SELECT
                            platform,
                            brand_name,
                            product_id,
                            product_name,
                            product_url,
                            cat
                        FROM 
                            cleaned_data.fcv_brand_search
                        WHERE
                            platform LIKE 'tiki%'
                        ''', 
                        con=engine
                      )

def fcv_product_review(engine):
    request_list = []
    data = get_data(engine)
    raw_url = "https://tiki.vn/api/v2/reviews?product_id={}&sort=bought&page={}&limit=10&include=comments"
    for _, product in data.iterrows():
        num_page = get_num_page(raw_url.format(product['product_id'], 1))
        for page in range(num_page):
            request_list.append(
                        {
                        "url": raw_url.format(product['product_id'], page + 1),
                        "request_type": "{}_product_review".format(product['platform']),
                        "platform": product['platform'],
                        "venture": "vn",
                        "database": 'fcv',
                        "schema": "raw_data",
                        "table": "{}_product_review_v1".format(product['platform']),
                        "brand_name": product['brand_name'],
                        "product_id": product['product_id'],
                        "product_name": product['product_name'],
                        "product_url": product['product_url'],
                        "cat": product['cat'],
                        "page": page + 1,
                        "use_proxy": False
                        } 
                    ) 
    return {"urls": request_list}

