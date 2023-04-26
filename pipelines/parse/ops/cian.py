from dagster import asset
from utils.utils import get_cian_item_info
import pandas as pd
import numpy as np 
import time
from bs4 import BeautifulSoup
from io import BytesIO




@asset(
    name = 'cian_page_list',
    description='Получение сырых HTML',
    group_name='Download',
    compute_kind='HTML',
    required_resource_keys={"parser_resource"})
def fetch_cian(context) -> list:

    parser = context.resources.parser_resource
    url = context.op_config['start_url']
    parser.get('https://google.com')
    parser.get('https://ya.ru')
    parser.get('https://cian.ru')
    page_list = []
    
    for page in range(1, context.op_config['fetch_pages']):

        response = parser.get(url)
        context.log.info(len(response))
        time.sleep(np.random.poisson(2))

        page_list.append(BytesIO(response.encode()))

        url = url.replace(f'&p={page}', f'&p={page+1}')
    

    return page_list




# @asset(name = 'cian_dataframe',
#        compute_kind='bs4',
#        description='Парсинг итемов странички',
#        group_name='Extract')
# def convert_html_2_df_cian(cian_page_list:list) -> pd.DataFrame:

#     result_list = []
#     for page in cian_page_list:
        
#         items = BeautifulSoup(page, features='lxml').findAll('div',{'data-testid':'offer-card'})

#         tmp_result = [get_cian_item_info(item=item) for item in items]
#         result_list.extend(tmp_result)
        
#     result = pd.DataFrame(result_list)

#     return result



# @asset(name = 'featurized_cian_data',
#        compute_kind='Python',
#        description='Дополнение данных простыми фичами',
#        group_name='Featurize')
# def featuring_cian_data(cian_dataframe:pd.DataFrame)->pd.DataFrame:

#     cian_dataframe['price'] = cian_dataframe['price'].astype(float)
#     cian_dataframe[['rooms','m2','floor']] = cian_dataframe['title'].str.replace(',(?=\d)','.').str.split(',',expand=True)
#     cian_dataframe['m2'] = cian_dataframe['m2'].str.extract('(\d+).*м²').astype(float)
#     cian_dataframe[['floor','max_floor']] = cian_dataframe['floor'].str.extract('(\d+/\d+).*эт')[0].str.split('/',expand=True).astype(float)
#     cian_dataframe['text'] = cian_dataframe['text'].str.replace('\n+','').str.replace(' +',' ')
#     cian_dataframe['rubm2'] = cian_dataframe['price'] / cian_dataframe['m2']
#     cian_dataframe.drop(['title'],axis=1,inplace=True)

#     return cian_dataframe



# @asset(
#     name = 'save_cian_data',
#     description='Сохранение обогащенных данных в базенку',
#     group_name='Save',
#     compute_kind='SQL',
#     required_resource_keys={"db_resource"})
# def save_data_cian(context,featurized_cian_data:pd.DataFrame) -> None:

#     db = context.resources.db_resource
#     db.append_df(featurized_cian_data,'cian')
