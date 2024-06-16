import os
from io import StringIO
from helpers.extras import DAG
from airflow.decorators import task
from helpers.utils import split_to_chunks
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from connections.constants import S3__WRITE__REDSHIFT_MISC
from operations.supply_tools.scrapers.external_prices.profeco.transformers import get_city_values, get_cities
from helpers.constants import USER__JUAN_GOMEZ, USER__NICOLAS_NAVARRO


BUCKET_NAME: str = 'redshift-misc'
BASE_FOLDER: str = 'operations/scrapers/external_prices/profeco'
GET_CITIES__TASK_ID = 'get_cities'
GET_PRICES__TASK_ID = 'get_prices'
INSERT__TASK_ID = 'insert_data'

N_JOBS: int = 5


def get_s3_path(timestamp: str, city: str) -> str:
    return os.path.join(
        BASE_FOLDER,
        timestamp,
        f'{city}.csv'
    )


def to_s3(hook, data, timestamp: str, city: str):
    buffer = StringIO()
    data.to_csv(
        buffer,
        index=False
    )
    buffer.seek(0)

    key = get_s3_path(timestamp, city)

    hook.load_string(
        string_data=buffer.getvalue(),
        key=key,
        bucket_name=BUCKET_NAME
    )

    return key


@task(task_id=GET_PRICES__TASK_ID)
def get_prices(cities: dict, index: int, **context):
    inputs = cities['inputs']
    cities = split_to_chunks(list(cities['cities'].items()), N_JOBS)[index]

    results = []

    execution_id = str(context['execution_date'])

    for city_code, city in cities:
        hook = S3Hook(
            aws_conn_id=S3__WRITE__REDSHIFT_MISC,
        )

        key = get_s3_path(execution_id, city)

        if not hook.check_for_key(key, BUCKET_NAME):
            result = get_city_values(
                city=city,
                city_code=city_code,
                inputs=inputs
            )

            results.append(
                to_s3(
                    hook=hook,
                    data=result,
                    timestamp=execution_id,
                    city=city
                )
            )

        else:
            print(f"{key} already exists")
            results.append(
                key
            )

    return results


def job_task(cities: dict, job_id: int):
    with TaskGroup(group_id=f'subprocess_{job_id}', prefix_group_id=True) as group:
        get_prices(
            cities,
            index=job_id-1
        )

    return group


with DAG(schedule='20 06 * * *', owners=[USER__JUAN_GOMEZ, USER__NICOLAS_NAVARRO]) as dag:
    cities = task(
        get_cities,
        task_id=GET_CITIES__TASK_ID
    )()

    for i in range(N_JOBS):
        job_task(
            cities,
            job_id=i + 1
        )
