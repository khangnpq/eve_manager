import json, re 
from datetime import datetime
from html2text import html2text
from lxml import etree
import pandas as pd

def escape(text):
    text = text.replace('\\', ' ')
    text = text.replace('"', '')
    return text

def process_search_page(raw_data):
    row_list = []
    for _, record in raw_data.iterrows():
        html_text = etree.HTML(record['data'])
        brand_products = html_text.xpath('//script[@type="application/ld+json"]/text()')
        for product in brand_products:
            text = re.sub(r'("name": ")([\s\S]*?)("[,\n])', lambda x: x.group(1) + escape(x.group(2)) + x.group(3), product)
            text = re.sub(r'("description": ")([\s\S]*?)("[,\n])', lambda x: x.group(1) + escape(x.group(2)) + x.group(3), text)
            data = json.loads(text, strict=False)
    
            if data['@type'] == 'Product':
                product_url = data['url']
                product_id = re.findall('(?:p(\d*).html)', product_url)[0]
                product_url = 'https://tiki.vn/p{}.html'.format(product_id)
                row_list.append(
                    [
                        data['brand']['name'],  # brand_name
                        product_id,             # product_id
                        data['name'],           # product_name
                        product_url,            # product_url
                        record['data_key']      # data_key
                    ]
                                )
    return pd.DataFrame(
                        row_list, 
                        columns=[
                                'brand_name',
                                'product_id',
                                'product_name',
                                'product_url',
                                'data_key'
                                ]
                        )

class TikiCleaner:

    def __init__(self, raw_data):
        self.raw_data = raw_data

    ######################################################################################################

    def tiki_brand_search(self):
        cleaned_data = self.raw_data.drop('data', axis=1)
        right_df = process_search_page(self.raw_data)
        result = cleaned_data.merge(
                                    right_df,
                                    on=['brand_name','data_key'], 
                                    how='inner'
                                    )
        return result.drop_duplicates(subset=['product_url'])
    
    def tiki_product_info_v1(self):
        row_list = []
        cleaned_data = self.raw_data.drop('data', axis=1)
        for _, record in self.raw_data.iterrows():
            data = record['data']
            if len(re.findall(r'defaultProduct\s=\s({.*});', data)) > 0: 
                # print("defaultProduct")
                product_extract = re.findall(r'defaultProduct\s=\s({.*});', data)
                product_data = json.loads(product_extract[0]) 
            elif len(re.findall(r"(?:__NEXT_DATA__ = )(\{.*\});", data)) > 0 :
                # print("__NEXT_DATA__") 
                product_extract = re.findall(r"(?:__NEXT_DATA__ = )(\{.*\});", data) 
                product_data = json.loads(product_extract[0]) 
                product_data = product_data["props"]["initialState"]["desktop"]["product"]["data"] 
            if product_data is not None:
                breadcrumb = [breadcrumb['name'] for breadcrumb in product_data['breadcrumbs']]
                row_list.append(
                        [
                            # product_data['brand']['name'],                  # brand_name
                            product_data['current_seller'].get('store_id'),     # seller_id
                            product_data['current_seller'].get('name'),         # seller_name
                            product_data['id'],                             # product_id
                            product_data['name'],                           # product_name
                            product_data['stock_item']['qty'],              # stock
                            product_data['list_price'],                     # price_original
                            product_data['price'],                          # price_sale
                            product_data['discount_rate']/100,              # discount
                            breadcrumb,                                     # breadcrumb
                            record['data_key']                              # data_key
                        ]
                            )
        return cleaned_data.merge(
                                    pd.DataFrame(
                                        row_list, 
                                        columns=[
                                                # 'brand_name',
                                                'seller_id',
                                                'seller_name',
                                                'product_id',
                                                'product_name',
                                                'stock',
                                                'price_original',
                                                'price_sale',
                                                'discount',
                                                'breadcrumb',
                                                'data_key'
                                                ]
                                                ),
                                    on=['data_key'], how='inner'
                                    )
        # return cleaned_data
                
    def tiki_product_review_v1(self):
        row_list = []
        cleaned_data = self.raw_data.drop('data', axis=1)
        for _, record in self.raw_data.iterrows(): 
            data = json.loads(record['data'])['data']
            for review in data:
                review_created_at = datetime.fromtimestamp(review['created_at'])
                order_created_at = datetime.fromtimestamp(review['created_by']['purchased_at'])
                if 'timeline' in review.keys():
                    order_delivered_at = review['timeline']['delivery_date']
                else:
                    order_delivered_at = review_created_at
                row_list.append(
                    [
                    review['id'],                   # id
                    review_created_at,              # review_created_at
                    # review['seller']['id'],         # seller_id
                    # review['seller']['name'],       # seller_name
                    review['title'],                # title
                    review['content'],              # content
                    review['rating'],               # rating
                    order_created_at,               # order_created_at
                    order_delivered_at,             # order_delivered_at
                    record['data_key']              # data_key
                    ]
                                  )
        return cleaned_data.merge(
                        pd.DataFrame(
                                row_list,
                                columns = [
                                        'id',
                                        'review_created_at',
                                        # 'seller_id',
                                        # 'seller_name',
                                        'title',
                                        'content',
                                        'rating',
                                        'order_created_at',
                                        'order_delivered_at',
                                        'data_key'
                                        ]
                                    ),
                        on=['data_key'], 
                        how='inner'            
                                 )

    ######################################################################################################        
    
    def tiki_search_info_v1(self): 
        row_list = []
        cleaned_data = self.raw_data.drop('data', axis=1)
        for _, record in self.raw_data.iterrows(): 
            data = json.loads(record['data'])['data']
            for product in data:
                row_list.append(
                        [
                            product['id'],                             # product_id
                            product['name'],                           # product_name
                            product['stock_item']['qty'],              # stock
                            record['data_key']                         # data_key
                        ]
                                )
        right_df = pd.DataFrame(
                                row_list,
                                columns = [
                                        'product_id',
                                        'product_name',
                                        'stock',
                                        'data_key'
                                        ]
                                )
        result = cleaned_data.merge(
                                    right_df,
                                    on=['data_key'], 
                                    how='inner'
                                    )
        result['position'] = result.groupby('keyword').cumcount() + 1
        return result

    def tiki_category_info_v1(self):
        cleaned_data = self.raw_data.drop('data', axis=1)
        right_df = process_search_page(self.raw_data)
        result = cleaned_data.merge(
                                    right_df,
                                    on=['data_key'], 
                                    how='inner'
                                    )
        result['position'] = result.groupby('category_name').cumcount() + 1
        return result

    ######################################################################################################
    
    def tiki_flash_sale_v1(self):
        row_list = []
        cleaned_data = self.raw_data.drop('data', axis=1)
        for _, record in self.raw_data.iterrows(): 
            data = json.loads(record['data'])['data']
            for flash_sale in data:
                end_time = datetime.fromtimestamp(flash_sale['special_to_date'])
                price_sale = flash_sale['product']['price']
                price_original = flash_sale['product']['list_price']
                discount = (price_original - price_sale) / price_original
                product_url = 'https://tiki.vn/{}'.format(flash_sale['product']['url_path'])
                row_list.append(
                    [
                    flash_sale['from_date'],                    # start_time
                    end_time,                                   # end_time
                    flash_sale['deal_id'],                      # id
                    flash_sale['product'].get('sp_seller_id'),      # seller_id
                    flash_sale['product'].get('sp_seller_name'),    # seller_name
                    flash_sale['product'].get('master_id'),         # product_id
                    flash_sale['product'].get('name'),              # product_name
                    product_url,                                # product_url
                    price_original,                             # price_original
                    price_sale,                                 # price_sale
                    discount,                                   # discount
                    flash_sale['progress'].get('qty'),              # quantity
                    flash_sale['progress'].get('qty_ordered'),      # sold
                    flash_sale['progress'].get('qty_remain'),       # stock
                    record['data_key']                          # data_key
                    ]
                                  )
        return cleaned_data.merge(
                        pd.DataFrame(
                                row_list,
                                columns = [
                                        'start_time',
                                        'end_time',
                                        'id',
                                        'seller_id',
                                        'seller_name',
                                        'product_id',
                                        'product_name',
                                        'product_url',
                                        'price_original',
                                        'price_sale',
                                        'discount',
                                        'quantity',
                                        'sold',
                                        'stock',
                                        'data_key'
                                        ]
                                    ),
                        on=['data_key'], 
                        how='inner'            
                                 )

    # def tiki_campaign(self): 
        # data = json.loads(raw_data.xpath('//div[@id="page-builder"]/@data-elements').extract_first()) 

        # for section in data:
        #     proxy_api = proxy_generator() 
        #     if section.get("type") == 'deal': 
        #         yield scrapy.FormRequest("https://tiki.vn/api/v2/events/deals"
        #                                 , method = "GET"
        #                                 , formdata =    {
        #                                                 "tags": section.get("attributes").get("tags"),
        #                                                 "url": section.get("attributes").get("url"),
        #                                                 "limit": str(section.get("attributes").get("limit"))
        #                                                 }
        #                                 , callback = tiki_campaign_lv2_sku
        #                                 , meta = {
        #                                             "proxy": proxy_api,
        #                                             "campaign_url": raw_data.meta.get("url") 
        #                                         }
        #                                 )

        #     elif section.get("type") == 'category': 
        #         yield scrapy.FormRequest("https://tiki.vn/api/v2/landingpage/products"
        #                                 , method = "GET"
        #                                 , formdata =    {
        #                                                 "category_id": section.get("attributes").get("categoryId"),
        #                                                 "limit": str(section.get("attributes").get("limit")) 
        #                                                 }
        #                                 , callback = tiki_campaign_lv2_cat
        #                                 , meta = {
        #                                             "proxy": proxy_api,
        #                                             "campaign_url": raw_data.meta.get("url") 
        #                                         }
        #                                 )

        #     elif section.get('type') == 'coupon':
        #         yield scrapy.FormRequest("https://tiki.vn/api/v2/events/coupon"
        #                                 , method = "GET"
        #                                 , formdata =    {
        #                                                 "code": ','.join(section['attributes']['coupons']) 
        #                                                 }
        #                                 , callback = tiki_campaign_lv2_voucher
        #                                 , meta = {
        #                                             "proxy": proxy_api,
        #                                             "campaign_url": raw_data.url 
        #                                         }
        #                                 )

    # def tiki_campaign_lv2_sku(self): 
    #     data = json.loads(raw_data.text)

    #     for item in data["data"]: 
    #         proxy_api = proxy_generator()
    #         yield scrapy.Request(urljoin("https://tiki.vn", item.get("product").get("url_path"))
    #                             , callback = tiki_product
    #                             , meta  =   {
    #                                         "proxy": proxy_api,
    #                                         "campaign_url": raw_data.meta.get("campaign_url"),
    #                                         "table": "raw_data.tiki_campaign_product_info",
    #                                         "get_review": False 
    #                                         } 
    #                             ) 



    # def tiki_campaign_lv2_cat(self): 
    #     data = json.loads(raw_data.text)

    #     for item in data["data"]: 
    #         proxy_api = proxy_generator()
    #         yield scrapy.Request(urljoin("https://tiki.vn", item.get("url_path"))
    #                             , callback = tiki_product
    #                             , meta  =   {
    #                                         "proxy": proxy_api,
    #                                         "campaign_url": raw_data.meta.get("campaign_url"),
    #                                         "table": "raw_data.tiki_campaign_product_info",
    #                                         "get_review": False 
    #                                         } 
    #                             ) 



    # def tiki_campaign_lv2_voucher(self):
    #     data = json.loads(raw_data.text) 

    #     voucher_list = []
    #     if data.get("data"): 
    #         for item in data.get("data"): 
    #             info = {
    #                     "venture": "vn",
    #                     "platform": "lazada",
    #                     "created_at": datetime.now(), 
    #                     "updated_at": datetime.now(), 
    #                     "voucher_id": item.get("code", "N/A"), 
    #                     "data": {
    #                             "voucher_title": escape(item.get("code", "N/A")),
    #                             "end_time": item.get("expired_at", "N/A"),
    #                             "voucher_description": escape(item.get("description", "N/A")), 
    #                             "voucher_url": "https://tiki.vn/api/v2/events/coupon?code={}".format(item.get("code", "N/A")), 
    #                             "remaining_use": item.get("remain", "N/A"),
    #                             "brand_url": raw_data.meta.get("brand_url", "N/A")  
    #                             }
    #                     }
    #             voucher_list.append(info) 

    #     if len(voucher_list) > 0: 
    #         db.run_query(data_to_query("raw_data.tiki_campaign_voucher", voucher_list, multi_insert = True))
