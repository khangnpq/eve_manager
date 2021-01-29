import requests
import json 
import sqlalchemy as sa
import pandas as pd
from configparser import ConfigParser

def get_data(engine):
    return pd.read_sql(
                        '''
                        SELECT 
                            keyword,
                            brand as brand_name,
                            type as search_type,
                            priority
                        FROM 
                            "public"."dw_keywords"
                        ''', 
                        con=engine
                      )

def fcv_search_info(engine):
    request_list = []
    data = get_data(engine)
    for _, product in data.iterrows():
        for page in [1]:
            request_list.append(
                        {
                        "url": "https://tiki.vn/api/v2/products?limit=56&q={}&page={}".format(product['keyword'], page),
                        "request_type": "{}_search_info".format('tiki'),
                        "platform": 'tiki',
                        "venture": "vn",
                        "database": 'fcv',
                        "schema": "raw_data",
                        "table": "{}_search_info_v1".format('tiki'),
                        "keyword": product['keyword'],
                        "brand_name": product['brand_name'],
                        "search_type": product['search_type'],
                        "priority": product['priority'],
                        "page": page,
                        "use_proxy": False
                        } 
                    ) 
    return {"urls": request_list}

