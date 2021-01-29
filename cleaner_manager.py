from cleaners.tiki_cleaner import TikiCleaner
from alert_bot.alert import send_cleaner_error
import requests
import json
from configparser import ConfigParser
import sqlalchemy as sa
import pandas as pd
import traceback

def get_raw_data(engine, schema, table):
    return pd.read_sql(
                        f'''
                        DELETE FROM
                            "{schema}"."{table}"
                        RETURNING *
                        ''', 
                        con=engine
                      )

def get_old_data(engine, schema, table):
    return pd.read_sql(
                        f'''
                        DELETE FROM
                            "{schema}"."{table}"
                        WHERE
                            platform = 'tiki'
                        RETURNING *;
                        ''',
                        con = engine)

def clean_data(engine, clean_request, raw_data):
    mapping = {'left_only': 0, 'right_only': 1, 'both': 1} #is_active 0:False, 1:True
    cleaner = TikiCleaner(raw_data)
    try:
        cleaned_data = getattr(cleaner, clean_request['table'])()
        if clean_request['table'] == 'tiki_brand_search' and len(cleaned_data) > 0:
            old_cleaned_data = get_old_data(engine, 'cleaned_data', 'fcv_brand_search')
            try:
                old_cleaned_data.drop(['created_at', 'platform', 'venture', 
                                        'page', 'data_key', 'cat', 'is_active',
                                        'product_id', 'product_name'], axis=1, inplace=True)
                if len(old_cleaned_data):
                    result = old_cleaned_data.merge(cleaned_data, on=['brand_name', 'product_url'], how='outer', indicator='is_active')
                    result['is_active'] = result['is_active'].map(mapping)
                else:
                    result = cleaned_data
                    result['is_active'] = 1
                return result
            except:
                old_cleaned_data.to_sql(
                                        'fcv_brand_search', 
                                        con=engine, 
                                        if_exists='append', 
                                        schema='cleaned_data', 
                                        index=False
                                       )
                print(traceback.format_exc())
                return cleaned_data
        else:
            return cleaned_data
    except Exception as e:
        raw_data.to_sql(
                        clean_request['table'], 
                        con=engine, 
                        if_exists='append', 
                        schema=clean_request['schema'], 
                        index=False
                       )
        print(traceback.format_exc())
        return ''
    

if __name__ == '__main__':
    parser = ConfigParser()
    parser.read('credentials.ini')
    get_task_link = parser['task_manager']['get_data']
    post_task_link = parser['task_manager']['post_data']
    raw_clean_map = {
                     'tiki_brand_search': 'fcv_brand_search',
                     'tiki_product_info_v1': 'fcv_product_info',
                     'tiki_product_review_v1': 'fcv_product_review',
                     'tiki_search_info_v1': 'fcv_search_info',
                     'tiki_category_info_v1': 'fcv_category_info',
                     'tiki_flash_sale_v1': 'fcv_flash_sale',
                     'tiki_campaign': 'fcv__campaign'
                    }
    data = requests.get(get_task_link)
    clean_request_list = json.loads(data.text)['urls']
    clean_failed = []
    for _ in range(len(clean_request_list)):
        clean_request = clean_request_list.pop()
        failed = True
        if parser.has_section(clean_request['database']):
            db_auth = parser[clean_request['database']]
            engine = sa.create_engine('{}://{}:{}@{}:{}/{}'.format(*db_auth.values()))
            raw_data = get_raw_data(engine, clean_request['schema'], clean_request['table'])
            if len(raw_data):
                cleaned_data = clean_data(engine, clean_request, raw_data)
                if len(cleaned_data):
                    cleaned_data.to_sql(
                                        raw_clean_map[clean_request['table']], 
                                        con=engine, 
                                        if_exists='append', 
                                        schema='cleaned_data', 
                                        index=False
                                        )
                    failed = False
                    print('Parse successfully')
            else:
                print('Get raw data error')
                failed = False
        else:
            print('No database credential for this request')

        if failed:
            clean_failed.append(clean_request)

    if len(clean_failed):        
        json_dict = {"urls": clean_failed}
        requests.post(post_task_link, data=json.dumps(json_dict))