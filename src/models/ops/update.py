from dagster import asset,graph_multi_asset,AssetOut,op,Out,Output
import pandas as pd
from sklearn import model_selection,metrics
from catboost import CatBoostRegressor
import joblib

@asset(name = 'load_data',
       compute_kind='SQL',
       description='Сбор данных',
    #    required_resource_keys={"db_resource"},
       group_name='Extract')
def get_data(context) -> pd.DataFrame:

       df = pd.read_parquet('../data/sample_data.parquet').dropna(subset='rubm2')
       df = df.drop(['datetime','price','publish_delta','url','id','text','Город','title','img_list','metro_branch','metro_name','metro_dist'],axis=1)
       
       return df



@asset(name = 'general_clean_data',
       compute_kind='Python',
       description='Подготовка данных',
       group_name='DataPrepare')
def general_prepare(load_data):
    data = load_data.copy()
    data['rooms'] = data['rooms'].where(lambda x: x.isin(data['rooms'].value_counts().iloc[:6].index),'Other')
    
    data['postcode'] = data['postcode'].explode().astype(float).groupby(level=0).mean() // 100
    data['is_apart'] = data['rooms'].str.contains('апарт')
    data['n_rooms'] = data['rooms'].str.extract('(\d)').fillna(0).astype(float)
    return data



@asset(name = 'meta_features',
       compute_kind='Python',
       description='Получение мета данных о ценах',
       group_name='DataPrepare')
def get_meta_features(general_clean_data):
    

    general_clean_data['area_obj_cnt'] = general_clean_data.groupby('Округ')['rubm2'].transform('count')
    general_clean_data['metro_obj_cnt'] = general_clean_data.groupby('Метро')['rubm2'].transform('count')
    general_clean_data['district_obj_cnt'] = general_clean_data.groupby('Район')['rubm2'].transform('count')
    general_clean_data['Округ'] = general_clean_data.groupby('Округ')['rubm2'].transform('mean')
    general_clean_data['Метро'] = general_clean_data.groupby('Метро')['rubm2'].transform('mean')
    general_clean_data['Район'] = general_clean_data.groupby('Район')['rubm2'].transform('mean')

    return general_clean_data[['area_obj_cnt','metro_obj_cnt','district_obj_cnt','Округ','Метро','Район']]


@asset(name = 'advance_home_data',
       compute_kind='Python',
       description='Получение специфичных данных по дому',
       group_name='DataPrepare')
def adv_home_prepare(general_clean_data):

    feature_dict = general_clean_data['advanced_home_info'].apply(lambda x: pd.DataFrame(x).set_index('key')['value'].to_dict()).rename('advacned_info')
    result_dict = {}
    result_dict['year_of_build'] = feature_dict.apply(lambda x: x.get('Год_ввода_в_эксплуатацию'))
    result_dict['rent_counts'] = feature_dict.apply(lambda x: x.get('Количество_квартир'))
    result_dict['n_enterss'] = feature_dict.apply(lambda x: x.get('Количество_подъездов'))
    result_dict['m2_house'] = feature_dict.apply(lambda x: x.get('Площадь_многоквартирного_дома,_кв.м'))

    return pd.DataFrame(result_dict)



@asset(name = 'result_dataset',
       compute_kind='Python',
       description='Объединение данных',
       group_name='DataPrepare')
def data_pipeline(general_clean_data,
              advance_home_data,
                  meta_features
                  ):
    concated = general_clean_data.drop(['Округ','Метро','Район'],axis=1)\
                                   .join(advance_home_data)\
                                   .join(meta_features)
    result = concated.select_dtypes(exclude='O')
    
    return result


@op(name='split_data_function',
       out={
           'train_data':Out(),
           'test_data':Out(),
           'train_target':Out(),
           'test_target':Out(),
       })
def split_op_data(result_dataset):
    
    X = result_dataset.drop('rubm2',axis=1)
    Y = result_dataset['rubm2']

    x,xv,y,yv = model_selection.train_test_split(X,Y,train_size=.75)

    return x,xv,y,yv



@graph_multi_asset(name = 'split_data',
       group_name='Fit',
       outs={
           'train_data':AssetOut(),
           'test_data':AssetOut(),
           'train_target':AssetOut(),
           'test_target':AssetOut(),
       })
def split_data(result_dataset):
    

    x,xv,y,yv = split_op_data(result_dataset)


    return {
        'train_data':x,
           'test_data':xv,
           'train_target':y,
           'test_target':yv,
    }





@asset(name = 'fit_model',
       compute_kind='Python',
       description='Обучение модельки',
       group_name='Fit')
def fit_model(context,train_data,train_target):
       
    x,xv,y,yv = model_selection.train_test_split(train_data,train_target,train_size=.85)

    model = CatBoostRegressor(iterations=2000,verbose=500)
    model.fit(x,y,verbose=5000,eval_set=(xv,yv))

    return model




@asset(name = 'evaluate_model',
       compute_kind='Python',
       description='Проверка модельки',
       group_name='Evaluate')
def check_model_performanse(fit_model,test_data,test_target):
     
    metric_dict = {}
    clipped_idx = test_target.where(lambda x: x < x.quantile(.9)).reset_index(drop=True).dropna().index
    yhat = fit_model.predict(test_data)
    metric_dict.update({'mape':metrics.mean_absolute_percentage_error(test_target,yhat)})
    metric_dict.update({'rmse':metrics.mean_squared_error(test_target,yhat)**.5})
    metric_dict.update({'mae':metrics.mean_absolute_error(test_target,yhat)})
    
    metric_dict.update({'clipped_mape':metrics.mean_absolute_percentage_error(test_target.iloc[clipped_idx],yhat[clipped_idx])})
    metric_dict.update({'clipped_rmse':metrics.mean_squared_error(test_target.iloc[clipped_idx],yhat[clipped_idx])**.5})
    metric_dict.update({'clipped_mae':metrics.mean_absolute_error(test_target.iloc[clipped_idx],yhat[clipped_idx])})
    
    joblib.dump(fit_model,'../models/catboost_v1.joblib')

    return Output(None,metadata=metric_dict)



