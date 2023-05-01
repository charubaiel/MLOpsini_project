
from dagster import define_asset_job,schedule,sensor,load_assets_from_modules
from dagster import Definitions,DefaultSensorStatus,DefaultScheduleStatus,RunRequest
from ETLs.ops import parse,featurize
from ETLs.ops.parse import partitions,partition_keys
from utils.configs import S3Resource,DatabaseResource,ParserResource
import yaml
import numpy as np
from pathlib import Path

ROOT = Path(__file__).parent


with open(f'{ROOT}/config.yml') as buffer:
    config = yaml.safe_load(buffer)
    
    parse_config = config.copy()







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
                            tags={"dagster/max_retries": 3, 
                                "dagster/retry_strategy": "FROM FAILURE"}
                                )


@schedule(
    cron_schedule="12/23 9-22 * * *",
    job=parse_job,
    default_status=DefaultScheduleStatus.RUNNING,
    execution_timezone="Europe/Moscow",
)
def parsing_schedule():
    run_config = config.copy()
    run_config['ops']['page_list']['config']['fetch_pages'] = 1
    if np.random.beta(1,1) >= .5:
        for partition in partition_keys:
            yield RunRequest(
            run_key=partition,
            run_config=run_config,
            partition_key=partition
            )

@sensor(
    job=featurize_job,
    minimum_interval_seconds=60*10,
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
            "db": DatabaseResource(connection_path='../data/protodb.db'),
            'parser':ParserResource(),
            's3':S3Resource(),
               }
)

