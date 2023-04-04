
import pathlib
import warnings

import pandas as pd
import yaml
from bs4 import BeautifulSoup

warnings.simplefilter('ignore')
ROOT = pathlib.Path(__file__).parent.parent


with open(f'{ROOT}/pipelines/config.yml') as buffer:
    config = yaml.safe_load(buffer)




def get_item_info(item:BeautifulSoup):
    item_desc = {}
    try:
        item_desc['datetime'] = pd.to_datetime('now',utc=True)
        item_desc['publish_delta'] = item.find('div',{'data-marker':'item-date'}).text
        item_desc['id'] = item['id']
        item_desc['url'] = item.a['href']
        item_desc['title'] = item.a['title']
        item_desc['text'] = item.meta['content']
        item_desc['price'] = item.find('meta',{'itemprop':'price'})['content']
        try:
            item_desc['JK'] = item.find('div',{'data-marker':'item-development-name'}).text
        except:
            item_desc['JK'] = ''
        item_desc['adress'] = item.find('div',{'data-marker':'item-address'}).span.text
        item_desc['metro_dist'] = item.find('span',{'class':'geo-periodSection-bQIE4'}).text
        item_desc['metro'] = item.find('div',{'class':'geo-georeferences-SEtee text-text-LurtD text-size-s-BxGpL'}).text.replace(item_desc['metro_dist'],'')
        item_desc['metro_branch'] = item.find('i',{'class':'geo-icon-Cr9YM'})['style'].replace('background-color:','')
    except:
        pass

    return item_desc




def checksum_items(page:str) -> list:

    bs_data = BeautifulSoup(page, features='lxml')
    result = bs_data.find('div', {'class': 'items-items-kAJAg'}).findAll('div', {'data-marker': 'item'})

    new_items = [item for item in result ]
    

    return new_items
    
    