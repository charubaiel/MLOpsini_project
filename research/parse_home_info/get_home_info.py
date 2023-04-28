import pandas as pd
import time
from tqdm import tqdm
import numpy as np



base_url = 'https://dom.mingkh.ru'


for page in tqdm(range(1,328)):
    time.sleep(np.random.poisson(5))
    page_table = pd.read_html(f'https://dom.mingkh.ru/moskva/moskva/houses?page={page}',extract_links='body')[0]
    page_table[['Адрес','home_url']] = page_table['Адрес'].apply('|'.join).str.split('|',expand=True)
    page_table.assign(page=page).to_csv('base_home_data.csv',mode='a')

dd = pd.read_csv('data/base_home_data.csv')


def parse_home_page(home_url):
    df_list = pd.read_html(home_url)
    _id = home_url.split('/')[-1]
    home_data = pd.concat(df_list,axis=0)\
                    .iloc[:,:3].dropna(thresh=1)\
                    .drop_duplicates(subset=0)\
                    .set_index(0).fillna('')\
                    .fillna(method='ffill',axis=1).iloc[:,1]\
                    .rename(_id).rename_axis('id')
    return home_data.to_frame().T

# home_list = dd['home_url'].where(lambda x: x!='home_url').dropna().drop_duplicates()
# result_list = []

# for idx,url in tqdm(enumerate(home_list,1),total=len(home_list)):
#     result_list.append(parse_home_page(base_url+url))
#     if idx % 500 ==0:
#         pd.concat(result_list).to_hdf('detailed_data.h5',key=str(idx))
#         result_list = []

# async def get_url(session,url):
#     full_url = base_url + url       
#     response = await session.get(full_url)

#     if response.status_code == 200:
#         return response
#     print(response.status_code)

# async with httpx.AsyncClient(http2=True,timeout=30) as client:
#     tasks = []
#     for url in home_list.drop_duplicates().values:
#         tasks.append(get_url(client,url))

#     response_list = await asyncio.gather(*tasks,return_exceptions=True)