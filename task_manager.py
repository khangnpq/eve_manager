from tasks.fcv_brand_search import fcv_brand_search
from tasks.fcv_product_info import fcv_product_info
from tasks.fcv_category_info import fcv_category_info
from tasks.fcv_search_info import fcv_search_info
from tasks.fcv_product_review import fcv_product_review
from tasks.fcv_flash_sale import fcv_flash_sale
from configparser import ConfigParser
import sqlalchemy as sa
import requests
import json
import sys

#########################################################################################3

# log_path = os.path.dirname(os.path.realpath(__file__)) + '/log/swift247.log'
# logging.basicConfig(
#                     filename=log_path,
#                     level=logging.ERROR, 
#                     format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S',
#                     )

if __name__ == '__main__':
    task_mapping = {
                    "fcv_brand_search": fcv_brand_search,
                    "fcv_product_info": fcv_product_info,
                    "fcv_search_info": fcv_search_info,
                    "fcv_category_info": fcv_category_info,
                    "fcv_flash_sale": fcv_flash_sale,
                    # "fcv_campaign": fcv_campaign,
                    "fcv_product_review": fcv_product_review,
                   }
    # python task_manager.py fcv fcv_brand_search
    project = sys.argv[1] # 'fcv
    task = sys.argv[2] # 'fcv_brand_search'

    parser = ConfigParser()
    parser.read('credentials.ini')

    if parser.has_section('task_priority'):
        high_prior_task = json.loads(parser["task_priority"]["high"])
        high_prior_link = parser["task_priority"]["post_high"].format(project)
        low_prior_task = json.loads(parser["task_priority"]["low"])
        low_prior_link = parser["task_priority"]["post_low"].format(project)
    else:
        sys.exit("No task priority is defined")

    if parser.has_section(project):
        credentials = parser[project]
        engine = sa.create_engine('{}://{}:{}@{}:{}/{}'.format(*credentials.values()))
    else:
        sys.exit('No credential for project {}'.format(project))

    request_list = task_mapping[task](engine)

    if task in high_prior_task:
        link = high_prior_link
    elif task in low_prior_task:
        link = low_prior_link

    requests.post(link, data = json.dumps(request_list))
