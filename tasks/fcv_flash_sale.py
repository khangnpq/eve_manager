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

def fcv_flash_sale(engine):
    request_list = []
    raw_url = "https://tiki.vn/api/v2/widget/deals/mix?type=now&page={}&per_page=10"
    num_page = get_num_page(raw_url.format(1))
    for page in range(num_page):
        request_list.append(
                    {
                    "url": raw_url.format(page + 1),
                    "request_type": "{}_flash_sale".format('tiki'),
                    "platform": 'tiki',
                    "venture": "vn",
                    "database": 'fcv',
                    "schema": "raw_data",
                    "table": "{}_flash_sale_v1".format('tiki'),
                    "page": page + 1,
                    "use_proxy": False
                    } 
                ) 
    return {"urls": request_list}

