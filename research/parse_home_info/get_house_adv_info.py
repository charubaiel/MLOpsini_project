import pandas as pd
from tqdm.contrib.concurrent import process_map
from multiprocessing import Pool

base_url = 'https://dom.mingkh.ru'
base_data = pd.read_csv('base_home_data.csv')


def parse_home_page(home_url):
    full_url = base_url + home_url
    _id = home_url.split('/')[-1]
    df_list = pd.read_html(full_url)
    home_data = pd.concat(df_list)\
                        .iloc[:,:3]\
                        .dropna(thresh=1)\
                        .set_index(0)\
                        .fillna(method='ffill',axis=1)\
                        .iloc[:,1]\
                        .rename(_id)\
                        .rename_axis('id')
    return home_data.to_frame().T.to_csv('complete_home_info.csv',mode='a')

home_list = base_data['home_url'].where(lambda x: x!='home_url').dropna().drop_duplicates().tolist()




if __name__ == "__main__":
    process_map(parse_home_page,home_list,max_workers = 2,chunksize=100)    
#     asyncio.run(make_requests(urls=home_list))