
from dagster import Definitions,DefaultSensorStatus,RunRequest
from dagster import define_asset_job,sensor,load_assets_from_modules
from featurize.ops import cian
from utils.connections import db_resource,parser_resource,s3_resource
from pathlib import Path
import yaml

ROOT = Path(__file__).parent



with open(f'{ROOT}/config.yml') as buffer:
    config = yaml.safe_load(buffer)





featurize_job = define_asset_job(name='featurize_data',
                            config=config,
                            tags={"dagster/max_retries": 1, 
                                "dagster/retry_strategy": "FROM FAILURE"}
                                )

@sensor(
    job=featurize_job,
    minimum_interval_seconds=5,
    default_status=DefaultSensorStatus.RUNNING
)
def check_updates():
    has_new_data = len([i for i in Path(f'{ROOT.parent.parent}/data/raw').glob('*')])
    if has_new_data:
        return RunRequest()



all_assets = load_assets_from_modules([cian])

defs = Definitions(
    assets=all_assets,
    jobs=[featurize_job],
    sensors=[check_updates],
    resources={"db_resource": db_resource,
               'parser_resource':parser_resource,
               's3_resource':s3_resource,
               }
)

