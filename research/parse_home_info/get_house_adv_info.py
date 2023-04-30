import pandas as pd
from tqdm.contrib.concurrent import process_map
import pathlib

ROOT = pathlib.Path(__file__).parent
BASE_URL = 'https://dom.mingkh.ru'
BASE_FILEPATH = 'base_home_data.csv'


def parse_home_page(url: str):
    full_url = BASE_URL + url
    _id = url.split('/')[-1]
    df_list = pd.read_html(full_url)
    home_data = pd.concat(df_list)\
        .iloc[:, :3]\
        .dropna(thresh=1)\
        .set_index(0)\
        .fillna(method='ffill', axis=1)\
        .iloc[:, 1]\
        .rename(_id)\
        .rename_axis('id')
    return home_data.to_frame().T.assign(url=url)



def chunk_save(urls_to_load:list):
    result = []
    for idx,url in enumerate(urls_to_load):
        result.append(parse_home_page(url))
        if idx% 100 == 0 :
            pd.concat(result,axis=1).to_parquet(f'{idx}_homes.parquet')
            result = []
            





def get_items_to_load(base_file: str, loaded_file: str):

    base_data = pd.read_csv(base_file)
    urls_to_load = set(base_data['home_url'].where(
        lambda x: x != 'home_url').dropna().drop_duplicates().tolist())
    
    try:
        loaded_items = set(pd.concat([pd.read_parquet(i) for i in pathlib.Path(f'{ROOT}/home_parquet_data/').glob('*')])['url'])
        urls_to_load = list(urls_to_load - loaded_homes)
        print(f'items to load : {len(urls_to_load)}')
    return urls_to_load


if __name__ == "__main__":

    process_map(chunk_save,
                get_items_to_load(
                                BASE_FILEPATH,
                                ),
                max_workers=2, chunksize=10)
#     asyncio.run(make_requests(urls=home_list))
