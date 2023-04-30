from dagster import asset,StaticPartitionsDefinition
import numpy as np 
import time
from io import BytesIO
import hashlib

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