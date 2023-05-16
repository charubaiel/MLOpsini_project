import time
import warnings
from difflib import SequenceMatcher
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from tqdm import tqdm

tqdm.pandas()

MLFLOW_PORT = os.getenv('MLFLOW_PORT')
assert MLFLOW_PORT is not None



RATE_REQUEST_LIMIT = 1.44
SLEEP_SECONDS = 5
BASE_SEARCH_URL = 'https://dom.mingkh.ru/search?searchtype=house&address='
BASE_URL = 'https://dom.mingkh.ru'
UA = ('''Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ''' +
      '''(KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36''')

geolocator = Nominatim(
    timeout=10,
    user_agent=UA)
geocode = RateLimiter(geolocator.geocode,
                      min_delay_seconds=RATE_REQUEST_LIMIT,
                      max_retries=2,
                      error_wait_seconds=SLEEP_SECONDS)

warnings.simplefilter('ignore')




def retry(times: int = 2, sleep_timeout: float = SLEEP_SECONDS):
    """
    Retry Decorator

    """

    def decorator(func):

        def newfn(*args, **kwargs):
            attempt = 0
            sleep_timeout = SLEEP_SECONDS
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(
                        f'Exception {e} thrown when attempting to run {func}, attempt '
                        f'{attempt} of {times}')
                    time.sleep(sleep_timeout)
                    sleep_timeout *= 2
                    attempt += 1
            return func(*args, **kwargs)

        return newfn

    return decorator


def get_avito_item_info(item: BeautifulSoup):
    item_desc = {}
    try:
        item_desc['datetime'] = pd.to_datetime('now', utc=True)
        item_desc['publish_delta'] = item.find('div', {
            'data-marker': 'item-date'
        }).text
        item_desc['id'] = item['id']
        item_desc['url'] = item.a['href']
        item_desc['title'] = item.a['title']
        item_desc['text'] = item.meta['content']
        item_desc['price'] = item.find('meta',
                                       {'itemprop': 'price'})['content']
        try:
            item_desc['JK'] = item.find('div', {
                'data-marker': 'item-development-name'
            }).text
        except Exception as e:
            print(f'{e}')
            item_desc['JK'] = ''
        item_desc['adress'] = item.find('div', {
            'data-marker': 'item-address'
        }).span.text
        item_desc['metro_dist'] = item.find('span', {
            'class': 'geo-periodSection-bQIE4'
        }).text
        item_desc['metro'] = item.find(
            'div', {
                'class':
                'geo-georeferences-SEtee text-text-LurtD text-size-s-BxgeocodeL'
            }).text.replace(item_desc['metro_dist'], '')
        item_desc['metro_branch'] = item.find(
            'i', {'class': 'geo-icon-Cr9YM'})['style'].replace(
                'background-color:', '')
    except Exception as e:
        print(e)
        pass

    return item_desc


def get_cian_item_info(item: BeautifulSoup):
    item_desc = {}
    item_desc['datetime'] = pd.to_datetime('now', utc=True).timestamp()
    item_desc['title'] = item.find('span', {'data-mark': 'OfferTitle'}).text
    item_desc['price'] = item.find('span', {
        'data-mark': 'MainPrice'
    }).text.replace('\xa0₽', '').replace(' ', '')
    item_desc['publish_delta'] = item.find(
        'div', {
            'class': '_93444fe79c--relative--IYgur'
        }).text.replace('\n', '').replace('  ', ' ')
    item_desc['url'] = item.find('div', {'data-name': 'LinkArea'}).a['href']
    item_desc['id'] = item_desc['url'].split('/')[-2]
    item_desc['text'] = item.find('div', {'data-name': 'Description'}).text
    item_desc.update(
        dict(
            zip(('Город', 'Округ', 'Метро', 'Район', 'Улица', 'Дом'), [
                step.text
                for step in item.findAll('a', {'data-name': 'GeoLabel'})
            ])))
    try:
        item_desc['metro_branch'] = item.find(
            'div', {'data-name': 'UndergroundIconWrapper'})['style'].replace(
                'color: rgb', '').replace(';', '')
    except Exception:
        item_desc['metro_branch'] = ''

    try:
        item_desc['metro_name'] = [
            i for i in item.find('div', {
                'data-name': 'SpecialGeo'
            }).text.split('\n') if i != ''
        ][0]
    except Exception:
        item_desc['metro_name'] = ''
    try:
        item_desc['metro_dist'] = [
            i for i in item.find('div', {
                'data-name': 'SpecialGeo'
            }).text.split('\n') if i != ''
        ][1]
    except Exception:
        item_desc['metro_dist'] = ''

    item_desc['img_list'] = [
        img['src'] for img in item.find('div', {
            'data-name': 'Gallery'
        }).findAll('img') if img['src'].endswith('.jpg')
    ]

    return item_desc


@retry()
def search_home_url(adress_dict: dict) -> str:

    home_query = ' '.join(adress_dict.values()).replace(' ', '+')
    adress = adress_dict['Улица'] + ', ' + adress_dict['Дом']

    request_home_query = requests.utils.quote(home_query)
    request_url = BASE_SEARCH_URL + request_home_query

    search_table = pd.read_html(request_url, extract_links='body')[0]

    search_table[['Адрес', 'home_url']] = search_table['Адрес'].apply(
        '|'.join).str.split('|', expand=True)
    search_table['match'] = search_table['Адрес'].apply(
        lambda x: SequenceMatcher(None, x, adress).ratio())

    result = search_table.set_index('home_url')['match'].idxmax()

    return result


@retry()
def parse_home_page(url: str) -> pd.Series:
    time.sleep(RATE_REQUEST_LIMIT / 1.44)
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
        lambda x: x.loc[x.iloc[:, 0] != x.iloc[:, 1]]).set_index('id')

    return result.T.rename(columns=lambda x: x.replace(' ', '_')).assign(
        jkh_url=url).T.iloc[:, 0]


def get_advanced_home_data(name_series: pd.Series) -> dict:
    try:
        url_ = search_home_url(name_series)
        result = parse_home_page(url_)
        return result.to_dict()
    except Exception as e:
        print(e)
        return {'Год_ввода_в_эксплуатацию': -1}
