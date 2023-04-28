from dagster import asset
import pandas as pd
from bs4 import BeautifulSoup
from utils.utils import (get_cian_item_info,
                        get_geo_features,
                        get_text_features,
                        get_title_features
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
    
    for page in page_list:
        
        items = BeautifulSoup(page, features='lxml').findAll('div',{'data-testid':'offer-card'})

        tmp_result = [get_cian_item_info(item=item) for item in items]
        result_list.extend(tmp_result)
        
    result = pd.DataFrame(result_list)

    return result



@asset(name = 'featurized_cian_data',
       compute_kind='Python',
       description='Дополнение данных простыми фичами',
       group_name='Featurize')
def featuring_cian_data(cian_dataframe:pd.DataFrame)->pd.DataFrame:


    geo_features = get_geo_features(cian_dataframe.loc[:,['Улица','Дом']].fillna('').apply(', '.join,axis=1))
    text_features = get_text_features(cian_dataframe['text'].str.lower())
    title_features = get_title_features(cian_dataframe['title'].str.lower())
    
    cian_dataframe['price'] = cian_dataframe['price'].astype(float)
    cian_dataframe['rubm2'] = cian_dataframe['price'] / cian_dataframe['m2']
    
    cian_dataframe.drop(['title'],axis=1,inplace=True)
    

    return cian_dataframe.join(title_features).join(geo_features).join(text_features)



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
    

