
from dagster import Definitions,define_asset_job,schedule,load_assets_from_modules
from parse.ops import cian
from parse.ops.cian import partitions
from utils.connections import db_resource,parser_resource,s3_resource
import yaml
from pathlib import Path

ROOT = Path(__file__).parent


with open(f'{ROOT}/config.yml') as buffer:
    config = yaml.safe_load(buffer)



parse_job = define_asset_job(name='update_data',
                            config=config,
                            partitions_def=partitions,
                            tags={"dagster/max_retries": 1, 
                                "dagster/retry_strategy": "FROM FAILURE"}
                                )

@schedule(
    cron_schedule="34 */18 * * *",
    job=parse_job,
    execution_timezone="Europe/Moscow",
)
def parsing_schedule():
    return {}


all_assets = load_assets_from_modules([cian])

defs = Definitions(
    assets=all_assets,
    jobs=[parse_job],
    schedules=[parsing_schedule],
    resources={
            "db_resource": db_resource,
               'parser_resource':parser_resource,
               's3_resource':s3_resource,
               }
)

