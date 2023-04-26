from dagster import asset,op,job,graph
from utils.connections import db_resource, parser_resource
from utils.utils import get_avito_item_info,config_avito as config
import pandas as pd
import numpy as np 
import time
from bs4 import BeautifulSoup
from io import BytesIO



@asset(
    name = 'avito_page_list',
    description='Получение сырых HTML',
    compute_kind='Selenium',
    group_name='Download',
    required_resource_keys={"parser_resource"})
def fetch_avito(context) -> list :

    parser = context.resources.parser_resource
    url = context.op_config['start_url']
    parser.get('https://google.com')
    parser.get('https://ya.ru')
    parser.get('https://avito.ru')
    page_list = []
    
    for page in range(1, context.op_config['fetch_pages']):

        response = parser.get(url)
        context.log.info(len(response))
        time.sleep(np.random.poisson(2))
        if 'Продажа' not in response:
            break

        page_list.append(BytesIO(response.encode()))

        url = url.replace(f'&p={page}', f'&p={page+1}')
    

    return page_list




@asset(name = 'avito_dataframe',
       compute_kind='bs4',
       description='Парсинг итемов странички',
       group_name='Extract')
def convert_html_2_df_avito(avito_page_list:list) -> pd.DataFrame:

    result_list = []
    for page in avito_page_list:
        
        bs_data = BeautifulSoup(page, features='lxml')
        items = bs_data.find('div', {'class': 'items-items-kAJAg'}).findAll('div', {'data-marker': 'item'})

        tmp_result = [get_avito_item_info(item=item) for item in items]
        result_list.extend(tmp_result)
        
    result = pd.DataFrame(result_list)

    return result


@asset(name = 'featurized_avito_data',
       compute_kind='Python',
       description='Дополнение данных простыми фичами',
       group_name='Featurize')
def featuring_avito_data(avito_dataframe:pd.DataFrame)->pd.DataFrame:

    avito_dataframe['price'] = avito_dataframe['price'].astype(float)
    avito_dataframe['street'] = avito_dataframe['adress'].str.extract('(.*?), (?=\d.*)')
    avito_dataframe['is_new'] = avito_dataframe['JK'] == ''
    avito_dataframe['rooms'] = avito_dataframe['title'].str.extract('«(.*?),')
    avito_dataframe['m2'] = avito_dataframe['title'].str.extract(', (\d+).*м²').astype(float)
    avito_dataframe[['floor','max_floor']] = avito_dataframe['title'].str.extract('(\d+/\d+).*эт')[0].str.split('/',expand=True).astype(float)
    avito_dataframe['text'] = avito_dataframe['text'].str.replace('\n','')
    avito_dataframe['rubm2'] = avito_dataframe['price'] / avito_dataframe['m2']
    avito_dataframe.drop(['title'],axis=1,inplace=True)

    return avito_dataframe




@asset(
    name = 'save_avito_data',
    compute_kind='SQL',
    description='Сохранение обогащенных данных в базенку',
    group_name='Save',
    required_resource_keys={"db_resource"})

def save_data_avito(context,featurized_avito_data:pd.DataFrame) -> None:

    db = context.resources.db_resource
    db.append_df(featurized_avito_data,'avito')




################################























@op(
    name = 'save_avito_data_op',
    description='Сохранение обогащенных данных в базенку',
    required_resource_keys={"db_resource"})
def save_data(context,featurized_avito_data:pd.DataFrame) -> None:

    db = context.resources.db_resource
    db.append_df(featurized_avito_data,'avito')



@graph(
        name='cleaning_avito',
        description='Первичная обработка данных для хранения в бд'
)
def cleaning_data(html):
    raw_df = convert_html_2_df_avito(html)
    featurize_df = featuring_avito_data(raw_df)
    return featurize_df



@job(name='update_avito',
    config=config,
    resource_defs={
    'db_resource':db_resource,
    'parser_resource':parser_resource,
    },
    tags={"dagster/max_retries": 1, 
        "dagster/retry_strategy": "FROM FAILURE"})
def avito_upldate_job():
    html_data = fetch_avito()
    featurize_df = cleaning_data(html_data)
    save_data(featurize_df)