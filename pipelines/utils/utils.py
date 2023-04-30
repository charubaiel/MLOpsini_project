
import warnings
import re
import pandas as pd
from bs4 import BeautifulSoup
from geopy import distance
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from difflib import SequenceMatcher
import requests

BASE_SEARCH_URL = 'https://dom.mingkh.ru/search?searchtype=house&address='
BASE_URL = 'https://dom.mingkh.ru'


geolocator = Nominatim(user_agent="geo_features")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.44)

warnings.simplefilter('ignore')


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
        item_desc['metro'] = item.find('div',{'class':'geo-georeferences-SEtee text-text-LurtD text-size-s-BxgeocodeL'}).text.replace(item_desc['metro_dist'],'')
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
    
def get_geo_features(adresses:pd.Series) -> pd.DataFrame:
    
    result = {}
    _center = geocode('Москва Красная площадь').point
    indexes = adresses.progress_apply(geocode)
    postcode = indexes.apply(lambda x: re.findall('\d{5,}',x.address)[0] if x is not None else x)
    latitude = indexes.apply(lambda x: x.latitude if x is not None else x)
    longtitude = indexes.apply(lambda x: x.longitude if x is not None else x)
    centreness = indexes.apply(lambda x: distance.distance(x.point,_center).km if x is not None else x)

    result.update({
        'postcode':postcode,
        'lat':latitude,
        'long':longtitude,
        'dist_to_center':centreness}
    )

    return pd.DataFrame(result)

def get_text_features(text_series:pd.Series) -> pd.DataFrame:

    result = {}

    is_lot = text_series.str.contains('\d{5,}')
    is_jk = text_series.str.contains('жк')
    has_park = text_series.str.contains('\\bпарк\\b')
    wc_type = (text_series.str.extract('санузел (\w{2,})').iloc[:,0].fillna('') + 
                text_series.str.extract('(\w{2,}) санузел').iloc[:,0].fillna('')
    ).where(lambda x: x!='')

    result.update(
        {
            'is_lot':is_lot,
            'is_jk':is_jk,
            'wc_type':wc_type,
            'has_park':has_park,
        }
    )

    return pd.DataFrame(result)


def get_title_features(title_series:pd.Series) -> pd.DataFrame:

    result = {}
    rooms,m2,floor = title_series.str.replace(',(?=\d)','.').str.split(',',expand=True)
    floor,max_floor = floor.str.extract('(\d+/\d+).*эт')[0].str.split('/',expand=True).astype(float)
    m2 = m2.str.extract('(\d+).*м²').astype(float)
    is_max_floor = floor == max_floor

    result.update(
        {
            'rooms':rooms,
            'm2':m2,
            'floor':floor,
            'max_floor':max_floor,
            'is_max_floor':is_max_floor,
        }
    )

    return pd.DataFrame(result)

def search_home_url(adress_dict:dict):

    home_query = ' '.join(adress_dict.values()).replace(' ','+')
    adress = adress_dict['Улица'] + ', ' + adress_dict['Дом']


    request_home_query = requests.utils.quote(home_query)
    request_url = BASE_SEARCH_URL + request_home_query


    search_table = pd.read_html(request_url,extract_links='body')[0]
    search_table[['Адрес','home_url']] = search_table['Адрес'].apply('|'.join).str.split('|',expand=True)
    search_table['match'] = search_table['Адрес'].apply(lambda x: SequenceMatcher(None,x,adress).ratio())

    
    result = search_table.set_index('home_url')['match'].idxmax()

    return result

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
    
    result = home_data.reset_index().drop_duplicates().pipe(
        lambda x: x.loc[x.iloc[:,0]!=x.iloc[:,1]]
    ).set_index('id')
    return result.T.assign(url=url).T.iloc[:,0]



def get_advanced_home_data(name_series:pd.Series):
    url_ = search_home_url(name_series)
    result = parse_home_page(url_)
    return result.to_dict()

def get_advanced_home_features(home_adress_df):
    result_list = {}
    for idx in home_adress_df.index:
        result_list[idx] = get_advanced_home_data(home_adress_df.fillna('').loc[idx].to_dict())

    return pd.DataFrame(result_list).T