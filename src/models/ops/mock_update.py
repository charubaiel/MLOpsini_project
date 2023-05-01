from dagster import asset
import pandas as pd




@asset(name = 'load_data',
       compute_kind='SQL',
       description='Сбор данных',
       required_resource_keys={"db_resource"},
       group_name='Extract')
def get_data(context) -> list:

    db = context.resources.db_resource
    result = db.connection.query('select * from intel.cian').df()
    
    return result



@asset(name = 'prepare_data',
       compute_kind='Python',
       description='Обучение модельки',
       group_name='Fit')
def prepare_data(context,load_data):
    # TDB
    result = ...
    return result


@asset(name = 'fit_model',
       compute_kind='Python',
       description='Обучение модельки',
       group_name='Fit')
def fit_model(context,prepare_data):
    # TDB
    result = ...
    return result


@asset(name = 'evaluate_model',
       compute_kind='Python',
       description='Проверка модельки',
       group_name='Fit')
def check_model_performanse(context,fit_model):
    # TDB
    result = ...
    return result



