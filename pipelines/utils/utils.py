
import warnings
import geopy
import re
import pandas as pd
from bs4 import BeautifulSoup
from geopy import distance

gp = geopy.Nominatim(user_agent='geo_features',timeout=1.44)
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
    
def get_geo_features(adresses:pd.Series) -> pd.DataFrame:
    
    result = {}

    _center = gp.geocode('Москва Красная площадь').point
    indexes = adresses.progress_apply(gp.geocode)
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
