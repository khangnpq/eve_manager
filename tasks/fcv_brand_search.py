import requests
import json 
import sqlalchemy as sa
import pandas as pd
from configparser import ConfigParser

def get_data(engine):
    return pd.read_sql(
                        '''
                        SELECT 
                            CASE
                                WHEN platform = 'lazmall' THEN 'lazada'
                                WHEN platform = 'shopeemall' THEN 'shopee'
                                WHEN platform = 'tiki trading' THEN 'tiki'
                                ELSE platform
                            END AS platform,
                            brand as brand_name,
                            link AS product_url,
                            cat
                        FROM 
                            public."dw_store_lists_2021-01-21"
                        WHERE
                            platform LIKE 'tiki%'
                        ''', 
                        con=engine
                      )

def fcv_brand_search(engine):
    request_list = []
    data = get_data(engine)
    for _, product in data.iterrows():
        for page in [1, 2]:
            request_list.append(
                        {
                        "url": product['product_url'],
                        "request_type": "{}_brand_search".format(product['platform']),
                        "platform": product['platform'],
                        "venture": "vn",
                        "database": 'fcv',
                        "schema": "raw_data",
                        "table": "{}_brand_search".format(product['platform']),
                        "brand_name": product['brand_name'],
                        "page": page,
                        "cat": product['cat'],
                        "use_proxy": False
                        } 
                    ) 
    return {"urls": request_list}

