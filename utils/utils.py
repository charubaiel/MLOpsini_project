
import pathlib
import warnings

import pandas as pd
import yaml
from bs4 import BeautifulSoup

warnings.simplefilter('ignore')
ROOT = pathlib.Path(__file__).parent.parent


with open(f'{ROOT}/pipelines/config.yml') as buffer:
    config = yaml.safe_load(buffer)




def get_avito_item_info(item:BeautifulSoup):
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


def get_cian_item_info(item:BeautifulSoup):
    item_desc = {}
    item_desc['datetime'] = pd.to_datetime('now',utc=True).timestamp()
    item_desc['title'] = item.find('span',{'data-mark':'OfferTitle'}).text
    item_desc['price'] = item.find('span',{'data-mark':'MainPrice'}).text.replace('\xa0₽','').replace(' ','')
    item_desc['publish_delta'] = item.find('div',{'class':'_93444fe79c--relative--IYgur'}).text.replace('\n','').replace('  ',' ')
    item_desc['url'] = item.find('div',{'data-name':'LinkArea'}).a['href']
    item_desc['id'] = item_desc['url'].split('/')[-2]
    item_desc['text'] = item.find('div',{'data-name':'Description'}).text
    item_desc.update(dict(zip(('Город','Округ','Метро','Район','Улица','Дом'),[step.text for step in item.findAll('a',{'data-name':'GeoLabel'})])))
    try:
        item_desc['metro_branch'] = item.find('div',{'data-name':'UndergroundIconWrapper'})['style'].replace('color: rgb','').replace(';','')
        item_desc['metro_name'] = [i for i in item.find('div',{'data-name':'SpecialGeo'}).text.split('\n') if i !=''][0]
        item_desc['metro_dist'] = [i for i in item.find('div',{'data-name':'SpecialGeo'}).text.split('\n') if i !=''][1]
    except:
        item_desc['metro_branch'] = ''
        item_desc['metro_name'] = ''
        item_desc['metro_dist'] = ''
    item_desc['img_list'] = [img['src'] for img in item.find('div',{'data-name':'Gallery'}).findAll('img') if img['src'].endswith('.jpg')]

    return item_desc



def checksum_items(page:str) -> list:

    bs_data = BeautifulSoup(page, features='lxml')
    result = bs_data.find('div', {'class': 'items-items-kAJAg'}).findAll('div', {'data-marker': 'item'})

    new_items = [item for item in result ]
    

    return new_items
    
    