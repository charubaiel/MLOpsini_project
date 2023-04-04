from dagster import asset
from utils.utils import get_item_info,checksum_items
import pandas as pd
import numpy as np 
import time
from bs4 import BeautifulSoup
from io import BytesIO

@asset(
    name = 'avito_page_list',
    description='Получение сырых HTML',
    group_name='Download',
    required_resource_keys={"parser_resource"})
def fetch_avito(context) -> list:

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
        if 'Продажа квартир' not in parser.parser.find_element('xpath', '//*[@id="app"]/div/div[2]/div[2]/div[2]/div/h1').text:
            break

        page_list.append(BytesIO(response.encode()))

        url = url.replace(f'&p={page}', f'&p={page+1}')
    

    return page_list



@asset(name = 'avito_dataframe',
       description='Выделение только новых объявлений',
       group_name='Extract')
def convert_html_2_items(context,avito_page_list:list) -> pd.DataFrame:

    result_list = []
    for page in avito_page_list:
        
        bs_data = BeautifulSoup(page, features='lxml')
        items = bs_data.find('div', {'class': 'items-items-kAJAg'}).findAll('div', {'data-marker': 'item'})

        tmp_result = [get_item_info(item=item) for item in items]
        result_list.extend(tmp_result)
        
    result = pd.DataFrame(result_list)

    return result



@asset(name = 'featurized_data',
       description='Дополнение данных простыми фичами',
       group_name='Featurize')
def featuring_data(avito_dataframe:pd.DataFrame)->pd.DataFrame:

    avito_dataframe['price'] = avito_dataframe['price'].astype(float)
    avito_dataframe['street'] = avito_dataframe['adress'].str.extract('(.*?), (?=\d.*)')
    avito_dataframe['is_new'] = avito_dataframe['JK'] == ''
    avito_dataframe['n_rooms'] = avito_dataframe['title'].str.extract('«(.*?),')
    avito_dataframe['m2'] = avito_dataframe['title'].str.extract(', (\d+).*м²').astype(float)
    avito_dataframe[['floor','max_floor']] = avito_dataframe['title'].str.extract('(\d+/\d+).*эт')[0].str.split('/',expand=True).astype(float)
    avito_dataframe['text'] = avito_dataframe['text'].str.replace('\n','')
    avito_dataframe['rubm2'] = avito_dataframe['price'] / avito_dataframe['m2']
    avito_dataframe.drop(['title'],axis=1,inplace=True)

    return avito_dataframe




@asset(
    name = 'save_data',
    description='Сохранение обогащенных данных в базенку',
    group_name='Save',
    required_resource_keys={"db_resource"})
def save_data(context,featurized_data:pd.DataFrame) -> None:

    db = context.resources.db_resource
    db.append_df(featurized_data,'avito')
