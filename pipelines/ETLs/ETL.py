
from dagster import define_asset_job,schedule,sensor,load_assets_from_modules
from dagster import Definitions,DefaultSensorStatus,RunRequest
from ETLs.ops import parse,featurize
from ETLs.ops.parse import partitions
from utils.connections import db_resource,parser_resource,s3_resource
import yaml
from pathlib import Path

ROOT = Path(__file__).parent


with open(f'{ROOT}/config.yml') as buffer:
    config = yaml.safe_load(buffer)
    parse_config = config.copy()
    feature_config = config.copy()
    del feature_config['ops']

parse_assets = load_assets_from_modules([parse])
featurize_assets = load_assets_from_modules([featurize])



parse_job = define_asset_job(name='update_data',
                            config=parse_config,
                            selection=parse_assets,
                            partitions_def=partitions,
                            tags={"dagster/max_retries": 3, 
                                "dagster/retry_strategy": "FROM FAILURE"}
                                )

featurize_job = define_asset_job(name='featurize_data',
                            selection=featurize_assets,
                            config=feature_config,
                            tags={"dagster/max_retries": 3, 
                                "dagster/retry_strategy": "FROM FAILURE"}
                                )


@schedule(
    cron_schedule="34 */18 * * *",
    job=parse_job,
    execution_timezone="Europe/Moscow",
)
def parsing_schedule():
    return {}


@sensor(
    job=featurize_job,
    minimum_interval_seconds=5,
    default_status=DefaultSensorStatus.RUNNING
)
def check_updates():
    has_new_data = len([i for i in Path(f'{ROOT.parent.parent}/data/raw').glob('*')])
    if has_new_data:
        return RunRequest()




defs = Definitions(
    assets=parse_assets + featurize_assets,
    jobs=[parse_job,featurize_job],
    sensors=[check_updates],
    schedules=[parsing_schedule],
    resources={
            "db_resource": db_resource,
               'parser_resource':parser_resource,
               's3_resource':s3_resource,
               }
)

