from dagster import asset
import pandas as pd
from bs4 import BeautifulSoup
import re
from utils.utils import (get_cian_item_info,
                        distance,geocode,tqdm,
                        get_advanced_home_data
                        )




    





@asset(name = 'unprocessed_filenames',
       compute_kind='s3',
       description='Сбор данных',
       required_resource_keys={"s3_resource"},
       group_name='Extract')
def get_raw_data(context) -> list:

    s3 = context.resources.s3_resource
    page_list= s3.get_filenames('raw')
    
    return page_list





@asset(name = 'cian_dataframe',
       compute_kind='bs4',
       description='Парсинг итемов странички',
       required_resource_keys={"s3_resource"},
       group_name='Extract')
def convert_html_2_df_cian(context,unprocessed_filenames) -> pd.DataFrame:

    s3 = context.resources.s3_resource
    result_list = []
    page_list= [s3.get_data(name) for name in unprocessed_filenames]
    assert len(page_list)>0, 'No Files Found'
    
    for page in page_list:
        
        items = BeautifulSoup(page, features='lxml').findAll('div',{'data-testid':'offer-card'})

        tmp_result = []
        for idx,item in enumerate(items):
            try:
                tmp_result.append(get_cian_item_info(item=item) )
            except Exception as e:
                context.log.warning(f'ERROR: {e}\n\nItem_num: {idx}')
        
        result_list.extend(tmp_result)
        
    result = pd.DataFrame(result_list)
    
    result ['Дом'] = result['Дом'].fillna('').apply(lambda x: re.sub('(?!:\d)(К)',' корпус ',x))

    return result



@asset( compute_kind='Python',
       description='Получение фичей на основе гео данных',
       group_name='Featurize')
def geo_features(cian_dataframe:pd.DataFrame) -> pd.DataFrame:
    
    result = {}
    adresses = cian_dataframe.loc[:,['Улица','Дом']].fillna('').apply(', '.join,axis=1)
    _center = geocode('Москва Красная площадь').point
    geo_data = adresses.apply(geocode)

    postcode = geo_data.apply(lambda x: re.findall('\d{5,}',x.address)[0] if x is not None else x)
    latitude = geo_data.apply(lambda x: x.latitude if x is not None else x)
    longtitude = geo_data.apply(lambda x: x.longitude if x is not None else x)
    centreness = geo_data.apply(lambda x: distance.distance(x.point,_center).km if x is not None else x)

    result.update({
        'postcode':postcode,
        'lat':latitude,
        'long':longtitude,
        'dist_to_center':centreness}
    )

    return pd.DataFrame(result)



@asset( compute_kind='Python',
       description='Получение фичей из текста объявления',
       group_name='Featurize')
def text_features(cian_dataframe:pd.DataFrame) -> pd.DataFrame:

    result = {}
    text_series = cian_dataframe['text'].str.lower()

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


@asset( compute_kind='Python',
       description='Выделение фичей из тайтла объявления',
       group_name='Featurize')
def title_features(cian_dataframe:pd.DataFrame) -> pd.DataFrame:

    title_series = cian_dataframe['title'].str.lower().to_frame()
    title_series[['rooms','m2','floor']] = title_series['title'].str.replace(',(?=\d)','.').str.split(',',expand=True)

    title_series[['floor','max_floor']] = title_series['floor'].str.extract('(\d+/\d+).*эт').iloc[:,0].str.split('/',expand=True).astype(float)
    title_series['m2'] = title_series['m2'].str.extract('(\d+).*м²').astype(float)
    title_series['is_max_floor'] = title_series['floor'] == title_series['max_floor']


    return title_series


@asset( compute_kind='Python',
       description='Получение дополнительных фичей из базы МинЖКХ',
       group_name='Featurize')
def advanced_home_features(cian_dataframe:pd.DataFrame) -> pd.DataFrame:
    result_list = {}
    home_adress_df = cian_dataframe.loc[:,['Город','Округ','Улица','Дом']].fillna('')
    for idx in tqdm(home_adress_df.index,total=home_adress_df.shape[0]):
        result_list[idx] = get_advanced_home_data(home_adress_df.fillna('').loc[idx].to_dict())

    return pd.DataFrame(result_list).T




@asset(name = 'featurized_cian_data',
       compute_kind='Python',
       description='Дополнение данных простыми фичами',
       group_name='Featurize')
def featuring_cian_data(
                        cian_dataframe:pd.DataFrame,
                        title_features:pd.DataFrame,
                        geo_features:pd.DataFrame,
                        text_features:pd.DataFrame,
                        advanced_home_features:pd.DataFrame,
                        )->pd.DataFrame:

    cian_dataframe['price'] = cian_dataframe['price'].astype(float)
    cian_dataframe['rubm2'] = cian_dataframe['price'] / cian_dataframe['m2']
    
    cian_dataframe.drop(['title'],axis=1,inplace=True)
    
    result = cian_dataframe.join(title_features)\
                            .join(geo_features)\
                            .join(text_features)\
                            .join(advanced_home_features)
    return result






@asset(
    name = 'save_cian_data',
    description='Сохранение обогащенных данных в базенку',
    group_name='Save',
    compute_kind='SQL',
    required_resource_keys={"db_resource"})
def save_data_cian(context,featurized_cian_data:pd.DataFrame) -> str:

    db = context.resources.db_resource
    db.connection.execute('create schema if not exists intel')
    db.append_df(featurized_cian_data,'intel.cian')
    return 'ok'



@asset(name = 'clear_processed_filenames',
       compute_kind='s3',
       description='удаление хлама данных',
       required_resource_keys={"s3_resource"},
       group_name='Clean')
def remove_used_data(context,unprocessed_filenames,save_cian_data) -> None:
    if save_cian_data == 'ok':
        s3 = context.resources.s3_resource
        [s3.remove_data(file) for file in unprocessed_filenames]
    

