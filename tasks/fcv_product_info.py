import requests
import json 
import sqlalchemy as sa
import pandas as pd
from configparser import ConfigParser

def get_data(engine):
    return pd.read_sql(
                        '''
                        SELECT
                            platform,
                            brand_name,
                            product_url,
                            cat
                        FROM 
                            cleaned_data.fcv_brand_search
                        WHERE
                            platform LIKE 'tiki%'
                        ''', 
                        con=engine
                      )

def fcv_product_info(engine):
    request_list = []
    data = get_data(engine)
    for _, product in data.iterrows():
        request_list.append(
                    {
                    "url": product['product_url'],
                    "request_type": "{}_product_info".format(product['platform']),
                    "platform": product['platform'],
                    "venture": "vn",
                    "database": 'fcv',
                    "schema": "raw_data",
                    "table": "{}_product_info_v1".format(product['platform']),
                    "brand_name": product['brand_name'],
                    "cat": product['cat'],
                    "use_proxy": False
                    } 
                ) 
    return {"urls": request_list}

