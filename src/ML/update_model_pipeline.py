from dagster import define_asset_job, load_assets_from_modules
from dagster import Definitions
from ML.ops import update
from pathlib import Path
import yaml

ROOT = Path(__file__).parent

with open(f'{ROOT}/config.yml') as buffer:
    config = yaml.safe_load(buffer)

model_assets = load_assets_from_modules([update])

model_update_job = define_asset_job(name='update_model',
                                    config=config,
                                    selection=model_assets,
                                    tags={
                                        "dagster/max_retries": 1,
                                        "dagster/retry_strategy":
                                        "FROM FAILURE"
                                    })
efs = Definitions(
    assets=model_assets,
    jobs=[model_update_job],
    # resources={
    #         "db_resource": db_resource,
    #            'parser_resource':parser_resource,
    #            's3_resource':s3_resource,
    #            }
)
