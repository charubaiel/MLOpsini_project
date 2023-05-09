import re

import pandas as pd
from bs4 import BeautifulSoup
from dagster import asset
from geopy import distance
from tqdm import tqdm
from utils.configs import DatabaseResource, S3Resource
from utils.utils import geocode, get_advanced_home_data, get_cian_item_info


@asset(name='unprocessed_filenames',
       compute_kind='s3',
       description='Сбор данных',
       group_name='Extract')
def get_raw_data(context, s3: S3Resource) -> list:
    client = s3.get_client()
    page_list = client.get_filenames('raw')

    return page_list


@asset(name='cian_raw_df',
       compute_kind='bs4',
       description='Парсинг итемов странички',
       group_name='Extract')
def convert_html_2_df_cian(context, s3: S3Resource,
                           unprocessed_filenames: list) -> pd.DataFrame:

    client = s3.get_client()
    result_list = []
    page_list = [client.get_data(name) for name in unprocessed_filenames]
    assert len(page_list) > 0, 'No Files Found'

    for page in page_list:

        items = BeautifulSoup(page, features='lxml').findAll(
            'div', {'data-testid': 'offer-card'})

        tmp_result = []
        for idx, item in enumerate(items):
            try:
                tmp_result.append(get_cian_item_info(item=item))
            except Exception as e:
                context.log.warning(f'ERROR: {e}\n\nItem_num: {idx}')

        result_list.extend(tmp_result)

    result = pd.DataFrame(result_list)

    result['Дом'] = result['Дом'].fillna('').apply(
        lambda x: re.sub('(?!:\d)(К)', ' корпус ', x))
    result = result.astype({'price': float})

    return result


@asset(name='cian_df',
       compute_kind='bs4',
       description='Оставление уникальных значений',
       group_name='Extract')
def pass_new_data(context, db: DatabaseResource,
                  cian_raw_df: pd.DataFrame) -> pd.DataFrame:

    cian_df = cian_raw_df
    client = db.get_client()
    check_new = set(cian_raw_df[['url', 'price']].apply(tuple, axis=1))

    try:
        history_data = client.connection.execute(
            'select distinct url,price from intel.cian').df()
        check_old = set(history_data[['url', 'price']].apply(tuple, axis=1))
    except Exception as e:
        context.log.warning(f'MESSAGE : {e}\n\nNew table creation')
        client.connection.execute('create schema if not exists intel')
        check_old = set([])

    diff_ = check_new - check_old
    assert len(diff_) > 0, 'No New Updates to load'

    new_urls_filter = [url[0] for url in diff_]
    cian_df = cian_raw_df.loc[cian_raw_df['url'].isin(new_urls_filter)]

    return cian_df


@asset(compute_kind='Python',
       description='Получение фичей на основе гео данных',
       group_name='Featurize')
def geo_features(cian_df: pd.DataFrame) -> pd.DataFrame:

    result = {}
    adresses = cian_df.loc[:, ['Улица', 'Дом']].fillna('').apply(', '.join,
                                                                 axis=1)
    _center = geocode('Москва Красная площадь').point
    geo_data = adresses.progress_apply(geocode)

    postcode = geo_data.apply(lambda x: re.findall('\d{5,}', x.address)
                              if x is not None else x)
    latitude = geo_data.apply(lambda x: x.latitude if x is not None else x)
    longtitude = geo_data.apply(lambda x: x.longitude if x is not None else x)
    centreness = geo_data.apply(lambda x: distance.distance(x.point, _center).
                                km if x is not None else x)

    result.update({
        'postcode': postcode,
        'lat': latitude,
        'long': longtitude,
        'dist_to_center': centreness
    })

    return pd.DataFrame(result)


@asset(compute_kind='Python',
       description='Получение фичей из текста объявления',
       group_name='Featurize')
def text_features(cian_df: pd.DataFrame) -> pd.DataFrame:

    result = {}
    text_series = cian_df['text'].str.lower()

    is_lot = text_series.str.contains('\d{5,}')
    is_jk = text_series.str.contains('жк')
    has_park = text_series.str.contains('\\bпарк\\b')
    wc_type = (
        text_series.str.extract('санузел (\w{2,})').iloc[:, 0].fillna('') +
        text_series.str.extract('(\w{2,}) санузел').iloc[:, 0].fillna('')
    ).where(lambda x: x != '')

    result.update({
        'is_lot': is_lot,
        'is_jk': is_jk,
        'wc_type': wc_type,
        'has_park': has_park,
    })

    return pd.DataFrame(result)


@asset(compute_kind='Python',
       description='Выделение фичей из тайтла объявления',
       group_name='Featurize')
def title_features(cian_df: pd.DataFrame) -> pd.DataFrame:

    title_series = cian_df['title'].str.lower().to_frame()
    title_series[['rooms', 'm2', 'floor']] = title_series['title'].str.replace(
        ',(?=\d)', '.').str.split(',', expand=True).iloc[:, :3]

    title_series[['floor', 'max_floor']] = title_series['floor'].str.extract(
        '(\d+/\d+).*эт').iloc[:, 0].str.split('/', expand=True).astype(float)

    title_series['m2'] = title_series['m2'].str.extract('(\d+).*м²').astype(
        float)
    title_series['is_max_floor'] = title_series['floor'] == title_series[
        'max_floor']

    return title_series


@asset(compute_kind='Python',
       description='Получение дополнительных фичей из базы МинЖКХ',
       group_name='Featurize')
def advanced_home_features(cian_df: pd.DataFrame) -> pd.Series:
    result_list = {}
    home_adress_df = cian_df.loc[:,
                                 ['Город', 'Округ', 'Улица', 'Дом']].fillna('')
    for idx in tqdm(home_adress_df.index, total=home_adress_df.shape[0]):
        result_list[idx] = get_advanced_home_data(
            home_adress_df.fillna('').loc[idx].to_dict())

    return pd.Series(result_list, name='advanced_home_info')


@asset(name='featurized_cian_data',
       compute_kind='Python',
       description='Дополнение данных простыми фичами',
       group_name='Featurize')
def featuring_cian_data(
    cian_df: pd.DataFrame,
    title_features: pd.DataFrame,
    geo_features: pd.DataFrame,
    text_features: pd.DataFrame,
    advanced_home_features: pd.Series,
) -> pd.DataFrame:

    cian_df.drop(['title'], axis=1, inplace=True)

    result = cian_df.join(title_features)\
                            .join(geo_features)\
                            .join(text_features)\
                            .join(advanced_home_features)

    result['price'] = result['price'].astype(float)
    result['rubm2'] = result['price'] / result['m2']
    result = result.rename(columns=lambda x: x.replace(' ', '_'))

    return result


@asset(name='save_cian_data',
       description='Сохранение обогащенных данных в базенку',
       group_name='Save',
       compute_kind='SQL')
def save_data_cian(context, db: DatabaseResource,
                   featurized_cian_data: pd.DataFrame) -> str:
    client = db.get_client()
    client.append_df(featurized_cian_data, 'intel.cian')
    return 'ok'


@asset(name='clear_processed_filenames',
       compute_kind='s3',
       description='удаление хлама данных',
       group_name='Clean')
def remove_used_data(context, s3: S3Resource, unprocessed_filenames: list,
                     save_cian_data: str) -> None:
    if save_cian_data == 'ok':
        client = s3.get_client()
        [client.remove_data(file) for file in unprocessed_filenames]
