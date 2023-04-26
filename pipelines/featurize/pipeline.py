
from dagster import StaticPartitionsDefinition,Definitions,define_asset_job,schedule,load_assets_from_modules
from parse.ops import cian
from utils.connections import db_resource,parser_resource
from utils.utils import config


parse_job = define_asset_job(name='update_data',
                            config=config,
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
    resources={"db_resource": db_resource,
               'parser_resource':parser_resource}
)

