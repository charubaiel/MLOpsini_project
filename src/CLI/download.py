from utils.configs import ParserResource, S3Resource
import yaml
import hashlib
import time
from io import BytesIO
from multiprocessing import Pool
import numpy as np
import click

partition_keys = ['room1', 'room2', 'room3']


@click.command()
@click.option('--max_pages', default=10, help='Number of page parse.')
@click.option('--config_path',
              default='ETLs/config.yml',
              prompt='path to configs',
              help='download and save raw pages.')
def fetch_cian(partition: str,
               max_pages=10,
               config_path: str = 'ETLs/config.yml'):

    with open(config_path) as f:
        params = yaml.safe_load(f)
    op_config = params['ops']['page_list']['config']

    parser = ParserResource()
    s3 = S3Resource()

    s3_client = s3.get_client()
    client = parser.get_client()

    url_params = '&'.join([f'{k}={v}' for k, v in op_config['params'].items()])
    url = op_config['start_url'] + url_params
    url = url.replace('room1', f'{partition}')

    client.get('https://google.com')
    client.get('https://ya.ru')
    client.get('https://cian.ru')

    last_status_code = 200
    page = 1

    while (last_status_code == 200) | (page == max_pages):

        # response = client.get(url)
        response = client.parser.get(url)

        last_status_code = response.status_code
        response_html = response.text

        time.sleep(np.random.poisson(2))
        file = BytesIO(response_html.encode())
        url = url.replace(f'&p={page}', f'&p={page+1}')
        name = hashlib.md5(file.read()).hexdigest() + f'_{partition}.html'

        s3_client.save_file(bucket='raw', name=name, file=file)
        page += 1

        print(f'Partition : {partition} | Page_num: {page}')


if __name__ == '__main__':
    with Pool(3) as p:
        p.map(fetch_cian, partition_keys)
