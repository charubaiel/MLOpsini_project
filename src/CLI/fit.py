import pandas as pd
from sklearn import model_selection, metrics
from catboost import CatBoostRegressor
import mlflow
from mlflow.models.signature import infer_signature
from sklearn.impute import KNNImputer
from utils.loggers import _logger, logger
import click

mlflow.set_tracking_uri('http://localhost:8112')
try:
    mlflow.set_experiment('cian_rubm2_predict')
except:
    pass


@logger
def get_data() -> pd.DataFrame:

    df = pd.read_parquet('../data/sample_data.parquet').dropna(subset='rubm2')
    df = df.drop([
        'datetime', 'price', 'publish_delta', 'url', 'id', 'text', 'Город',
        'title', 'img_list', 'metro_branch', 'metro_name', 'metro_dist'
    ],
                 axis=1)

    return df


@logger
def general_prepare(load_data):
    data = load_data.copy()
    data['rooms'] = data['rooms'].where(
        lambda x: x.isin(data['rooms'].value_counts().iloc[:6].index), 'Other')

    data['postcode'] = data['postcode'].explode().astype(float).groupby(
        level=0).mean() // 100
    data['is_apart'] = data['rooms'].str.contains('апарт')
    data['n_rooms'] = data['rooms'].str.extract('(\d)').fillna(0).astype(float)
    return data


@logger
def get_meta_features(general_clean_data):

    general_clean_data['area_obj_cnt'] = general_clean_data.groupby(
        'Округ')['rubm2'].transform('count')
    general_clean_data['metro_obj_cnt'] = general_clean_data.groupby(
        'Метро')['rubm2'].transform('count')
    general_clean_data['district_obj_cnt'] = general_clean_data.groupby(
        'Район')['rubm2'].transform('count')
    general_clean_data['Округ'] = general_clean_data.groupby(
        'Округ')['rubm2'].transform('mean')
    general_clean_data['Метро'] = general_clean_data.groupby(
        'Метро')['rubm2'].transform('mean')
    general_clean_data['Район'] = general_clean_data.groupby(
        'Район')['rubm2'].transform('mean')

    return general_clean_data[[
        'area_obj_cnt', 'metro_obj_cnt', 'district_obj_cnt', 'Округ', 'Метро',
        'Район'
    ]]


@logger
def adv_home_prepare(general_clean_data):

    feature_dict = general_clean_data['advanced_home_info'].apply(
        lambda x: pd.DataFrame(x).set_index('key')['value'].to_dict()).rename(
            'advacned_info')
    result_dict = {}
    result_dict['year_of_build'] = feature_dict.apply(
        lambda x: x.get('Год_ввода_в_эксплуатацию'))
    result_dict['rent_counts'] = feature_dict.apply(
        lambda x: x.get('Количество_квартир'))
    result_dict['n_enterss'] = feature_dict.apply(
        lambda x: x.get('Количество_подъездов'))
    result_dict['m2_house'] = feature_dict.apply(
        lambda x: x.get('Площадь_многоквартирного_дома,_кв.м'))

    return pd.DataFrame(result_dict)


@logger
def data_pipeline(general_clean_data: pd.DataFrame,
                  advance_home_data: pd.DataFrame,
                  meta_features: pd.DataFrame,
                  nan_inputer: bool = True):
    concated = general_clean_data.drop(['Округ', 'Метро', 'Район'], axis=1)\
        .join(advance_home_data)\
        .join(meta_features).select_dtypes(exclude='O')

    if nan_inputer:
        imputer = KNNImputer(n_neighbors=10, weights="distance")
        result = imputer.fit_transform(concated)
        return pd.DataFrame(result,
                            columns=concated.columns,
                            index=concated.index)

    return concated


@logger
def split_op_data(result_dataset):

    X = result_dataset.drop('rubm2', axis=1)
    Y = result_dataset['rubm2']

    x, xv, y, yv = model_selection.train_test_split(X,
                                                    Y,
                                                    train_size=.75,
                                                    random_state=1441)

    return x, xv, y, yv


@logger
def fit_model(train_data, train_target):

    x, xv, y, yv = model_selection.train_test_split(train_data,
                                                    train_target,
                                                    train_size=.85)

    model = CatBoostRegressor(iterations=10000,
                              loss_function='MAE',
                              verbose=500)
    model.fit(x, y, verbose=500, eval_set=(xv, yv))

    return model


@logger
def check_model_performanse(fit_model, test_data, test_target):

    with mlflow.start_run(run_name='base_test') as run:

        mlflow.autolog()

        metric_dict = {}

        clipped_idx = test_target.where(
            lambda x: x < x.quantile(.9)).reset_index(drop=True).dropna().index
        yhat = fit_model.predict(test_data)
        metric_dict.update({
            'mape':
            metrics.mean_absolute_percentage_error(test_target, yhat)
        })
        metric_dict.update(
            {'rmse': metrics.mean_squared_error(test_target, yhat)**.5})
        metric_dict.update(
            {'mae': metrics.mean_absolute_error(test_target, yhat)})

        metric_dict.update({
            'clipped_mape':
            metrics.mean_absolute_percentage_error(
                test_target.iloc[clipped_idx], yhat[clipped_idx])
        })
        metric_dict.update({
            'clipped_rmse':
            metrics.mean_squared_error(test_target.iloc[clipped_idx],
                                       yhat[clipped_idx])**.5
        })
        metric_dict.update({
            'clipped_mae':
            metrics.mean_absolute_error(test_target.iloc[clipped_idx],
                                        yhat[clipped_idx])
        })

        singaturka = infer_signature(test_data, test_target)

        model_info = mlflow.catboost.log_model(
            fit_model,
            artifact_path='models',
            registered_model_name="catboost_v1",
            signature=singaturka)

        # mlflow.evaluate(
        #     model=model_info.model_uri,
        #     data=test_data.assign(rubm2=test_target.values),
        #     targets="rubm2",
        #     model_type="r@loggeregressor",
        #     evaluators=["default"],
        # )

        mlflow.log_metrics(metric_dict)


@logger
@click.command()
def fit_model_pipeline():
    data = get_data()
    df = general_prepare(data)
    meta_features = get_meta_features(df)
    home_features = adv_home_prepare(df)
    result_df = data_pipeline(df, home_features, meta_features, True)
    x, xv, y, yv = split_op_data(result_df)
    model = fit_model(x, y)
    check_model_performanse(model, xv, yv)
