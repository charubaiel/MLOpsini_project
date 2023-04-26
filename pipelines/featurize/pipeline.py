
from dagster import Definitions,define_asset_job,sensor,load_assets_from_modules
from featurize.ops import cian
from utils.connections import db_resource,parser_resource,s3_resource
from utils.utils import config
from pathlib import Path

featurize_job = define_asset_job(name='featurize_data',
                            config=config,
                            tags={"dagster/max_retries": 1, 
                                "dagster/retry_strategy": "FROM FAILURE"}
                                )

@sensor(
    job=featurize_job,
    minimum_interval_seconds=30,
)
def check_updates():
    cwd = Path(__file__).parent.parent.parent
    if next(Path(f'{cwd}/data/raw').glob('*')) is not None:
        return {}



all_assets = load_assets_from_modules([cian])

defs = Definitions(
    assets=all_assets,
    jobs=[featurize_job],
    sensors=[check_updates]
    resources={"db_resource": db_resource,
               'parser_resource':parser_resource,
               's3_resource':s3_resource,
               }
)

