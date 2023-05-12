import hashlib
import time
from io import BytesIO

import numpy as np
from dagster import StaticPartitionsDefinition, asset
from utils.configs import ParserResource, S3Resource

partition_keys = ['room1', 'room2', 'room3']
partitions = StaticPartitionsDefinition(partition_keys)


@asset(
    name='page_list',
    description='Получение сырых HTML',
    group_name='Download',
    compute_kind='HTML',
    partitions_def=partitions,
)
def fetch_cian(context, parser: ParserResource) -> list:

    client = parser.get_client()
    partition = context.asset_partition_key_for_output()

    url_params = '&'.join(
        [f'{k}={v}' for k, v in context.op_config['params'].items()])
    url = context.op_config['start_url'] + url_params
    url = url.replace('room1', f'{partition}')

    context.log.warning(url)
    client.get('https://google.com')
    client.get('https://ya.ru')
    client.get('https://cian.ru')
    page_list = []

    for page in range(1, context.op_config['fetch_pages'] + 1):

        response = client.get(url)
        context.log.info(len(response))
        time.sleep(np.random.poisson(2))

        page_list.append(response.encode())

        url = url.replace(f'&p={page}', f'&p={page+1}')

    return page_list


@asset(
    name='raw_page_save_s3',
    description='Сохранение сырых страниц в s3',
    group_name='Save',
    compute_kind='S3',
    partitions_def=partitions,
)
def save_data_s3(context, s3: S3Resource, page_list: list) -> None:

    client = s3.get_client()
    partition = context.asset_partition_key_for_output()
    for page in page_list:

        name = hashlib.md5(page).hexdigest() + f'_{partition}.html'
        client.save_file(bucket='raw', name=name, file=page)
