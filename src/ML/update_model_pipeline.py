from dagster import define_asset_job, schedule, sensor, load_assets_from_modules
from dagster import Definitions, DefaultSensorStatus, RunRequest
from ML.ops import update
import yaml
from pathlib import Path

ROOT = Path(__file__).parent

# with open(f'{ROOT}/config.yml') as buffer:
#     config = yaml.safe_load(buffer)

model_assets = load_assets_from_modules([update])

model_update_job = define_asset_job(
    name='update_model',
    # config=config,
    selection=model_assets,
    tags={
        "dagster/max_retries": 1,
        "dagster/retry_strategy": "FROM FAILURE"
    })

# @schedule(
#     cron_schedule="34 */18 * * *",
#     job=model_update_job,
#     execution_timezone="Europe/Moscow",
# )
# def parsing_schedule():
#     return {}

# @sensor(
#     job=model_update_job,
#     minimum_interval_seconds=5,
#     default_status=DefaultSensorStatus.STOPPED
# )
# def check_updates():
#     has_new_data = len([i for i in Path(f'{ROOT.parent.parent}/data/raw').glob('*')])
#     if has_new_data:
#         return RunRequest()

defs = Definitions(
    assets=model_assets,
    jobs=[model_update_job],
    # sensors=[check_updates],
    # schedules=[parsing_schedule],
    # resources={
    #         "db_resource": db_resource,
    #            'parser_resource':parser_resource,
    #            's3_resource':s3_resource,
    #            }
)
