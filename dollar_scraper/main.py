from helpers.extras import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from helpers.constants import USER__NICOLAS_NAVARRO
from connections.aws.redshift import RedshiftWriteDataProdCo
import operations.supply_tools.scrapers.dollar_scraper.transformers as transformers


FINAL_TABLE = 'supply.fx_rate'
STAGING_TABLE = 'mongo_temp.fx_rate__stg'

CREATE_STG_TABLES__TASK_ID: str = 'create_stg_tables'
INGEST_DATA_DOLLAR__TASK_ID: str = 'ingest_data'
UPDATE_INSERT_FX_RATE__TASK_ID: str = 'update_and_insert_fx_rate'
DROP_TABLE__TASK_ID: str = 'drop_tables_stg'

CREATE_TABLE__STG: str = 'create/fx_rate.sql'
UPDATE_FINAL_TABLE: str = 'update/fx_rate.sql'
INSERT_FINAL_TABLE: str = 'insert/fx_rate.sql'


@task(task_id=INGEST_DATA_DOLLAR__TASK_ID)
def insert_data_stg(country: str, staging_table: str):
    transformer = getattr(
        transformers,
        f'{country.title()}TRM'
    )()

    transformer.transform()
    data = transformer.to_dict()

    with RedshiftWriteDataProdCo().get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute_drop_table(staging_table)
            cursor.execute(
                dag.read_file(
                    f'sources/sql/create/fx_rate.sql'
                ).format(
                    staging_table=staging_table
                )
            )

            cursor.execute_bulk_insert(
                data=[
                    list(data.values())
                ],
                columns=list(data.keys()),
                table=staging_table
            )

        conn.commit()


def execute(current_dag: DAG, path: str, staging_table: str):
    with RedshiftWriteDataProdCo().get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                current_dag.read_file(
                    path
                ).format(
                    staging_table=staging_table,
                    final_table=FINAL_TABLE
                )
            )
        conn.commit()


@task(task_id=UPDATE_INSERT_FX_RATE__TASK_ID)
def update_insert(current_dag: DAG, staging_table: str):
    execute(current_dag, f'sources/sql/{UPDATE_FINAL_TABLE}', staging_table)
    execute(current_dag, f'sources/sql/{INSERT_FINAL_TABLE}', staging_table)


@task(task_id=DROP_TABLE__TASK_ID)
def drop_table(table: str):
    with RedshiftWriteDataProdCo().get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute_drop_table(table)

        conn.commit()


with DAG(
        schedule='22 22 * * *',
        owners=[USER__NICOLAS_NAVARRO]
) as dag:
    for country in ['COL', 'PER', 'BRA', 'MEX']:
        with TaskGroup(group_id=country) as group:
            staging_table = f"{STAGING_TABLE}_{country.lower()}"

            insert_data_stg(
                country=country,
                staging_table=staging_table
            ) >> update_insert(
                current_dag=dag,
                staging_table=staging_table
            ) >> drop_table(
                staging_table
            )
