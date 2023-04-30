from dagster import asset,StaticPartitionsDefinition
import numpy as np 
import time
from io import BytesIO
import hashlib
import pandas as pd

partition_keys = ['room1','room2','room3']
partitions = StaticPartitionsDefinition(partition_keys)

@asset(
    name = 'page_list',
    description='Получение сырых HTML',
    group_name='Download',
    compute_kind='HTML',
    partitions_def=partitions,
    required_resource_keys={"parser_resource"})
def fetch_cian(context) -> list:

    parser = context.resources.parser_resource
    partition = context.asset_partition_key_for_output()
    params = '&'.join([f'{k}={v}' for k,v in context.op_config['params'].items()])
    url = context.op_config['start_url'] + params
    url = url.replace('room1',f'{partition}')
    context.log.warning(url)
    parser.get('https://google.com')
    parser.get('https://ya.ru')
    parser.get('https://cian.ru')
    page_list = []
    
    for page in range(1, context.op_config['fetch_pages']+1):
 
        response = parser.get(url)
        context.log.info(len(response))
        time.sleep(np.random.poisson(2))

        page_list.append(BytesIO(response.encode()))

        url = url.replace(f'&p={page}', f'&p={page+1}')
    

    return page_list



       
@asset(
    name = 'raw_page_save_db',
    description='Сохранение обогащенных данных в базенку',
    group_name='Save',
    compute_kind='SQL',
    partitions_def=partitions,
    required_resource_keys={"db_resource"}
    )
def save_data_db(context,page_list:list) -> None:
    
    result_ = {
        'page_hash':[],
        'page_html':[],
        'update_date':[pd.to_datetime('now',utc=True).value]*len(page_list),
    }
    for page in page_list:
        result_['page_hash'].append(hashlib.md5(page.read()).hexdigest())
        result_['page_html'].append(page.read())
        
    result_df = pd.DataFrame(result_)
    db = context.resources.db_resource
    db.connection.execute('create schema if not exists raw')
    db.append_df(result_df,'raw.cian') 


   
@asset(
    name = 'raw_page_save_s3',
    description='Сохранение сырых страниц в s3',
    group_name='Save',
    compute_kind='S3',
    partitions_def=partitions,
    required_resource_keys={"s3_resource"}
    )
def save_data_s3(context,page_list:list) -> None:
    
    s3 = context.resources.s3_resource
    partition = context.asset_partition_key_for_output()
    for page in page_list:
        name = hashlib.md5(page.read()).hexdigest()+f'_{partition}.html'
        file = page
        s3.save_file(bucket='raw',name=name,file=file)
 
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
