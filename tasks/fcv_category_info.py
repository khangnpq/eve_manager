import requests
import json 
import sqlalchemy as sa
import pandas as pd
from configparser import ConfigParser

def get_data(engine):
    return pd.read_sql(
                        '''
                        SELECT
                            LOWER(platform) AS platform,
                            category_level,
                            category_name,
                            category_url
                        FROM 
                            public.dw_category_pages
                        WHERE
                            LOWER(platform) LIKE 'tiki%'
                        ''', 
                        con=engine
                      )

def fcv_category_info(engine):
    request_list = []
    data = get_data(engine)
    for _, product in data.iterrows():
        for page in [1]:
            request_list.append(
                        {
                        "url": product['category_url'] + '?page={}'.format(page),
                        "request_type": "{}_category_info".format(product['platform']),
                        "platform": product['platform'],
                        "venture": "vn",
                        "database": 'fcv',
                        "schema": "raw_data",
                        "table": "{}_category_info_v1".format(product['platform']),
                        "category_level": product['category_level'],
                        "category_name": product['category_name'],
                        "page": page,
                        "use_proxy": False
                        } 
                    ) 
    return {"urls": request_list}
