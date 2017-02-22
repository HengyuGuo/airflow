from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    FBSnowflakeCreateStageOperator,
    FBS3KeySensor,
    FBS3ToSnowflakeOperator,
)
from airflow.hooks import FBCachedDbApiHook

from datetime import datetime, timedelta
from scrapes.constants import SCRAPE_TABLES_TO_SKIP
from scrapes.utils import (
    get_data_s3_key,
    get_schema_s3_key,
)
from snowflake.constants import (
    S3_BUCKET,
    CSV_STAGE,
    STAGING_SCRAPES_SCHEMA,
    SNOWFLAKE_CONN_ID,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 26),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PARENT_DAG_NAME = 'snowflake_csv_scrapes'
SCHEDULE_INTERVAL = '@daily'

main_dag = DAG(
    PARENT_DAG_NAME,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
)

def get_scrape_subdag(table_name):
    dag = DAG(
        '{}.{}'.format(PARENT_DAG_NAME, table_name),
        default_args=default_args,
        schedule_interval=SCHEDULE_INTERVAL,
    )

    data_s3_key = get_data_s3_key(table_name)
    schema_s3_key = get_schema_s3_key(table_name)

    wait_for_data_s3_key = FBS3KeySensor(
        task_id='wait_for_data_s3_key',
        s3_key=data_s3_key,
        dag=dag,
    )
    wait_for_schema_s3_key = FBS3KeySensor(
        task_id='wait_for_schema_s3_key',
        s3_key=schema_s3_key,
        dag=dag,
    )

    upload_table = FBS3ToSnowflakeOperator(
        task_id='upload_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        table='{schema}."{table_name}_{today}"'.format(
            schema=STAGING_SCRAPES_SCHEMA,
            table_name=table_name,
            today='{{ ds }}',
        ),
        data_s3_key=data_s3_key,
        stage=CSV_STAGE,
        drop_and_create=True,
        schema_s3_key=schema_s3_key,
        dag=dag,
    )
    upload_table.set_upstream([wait_for_data_s3_key, wait_for_schema_s3_key])
    return dag

create_stage = FBSnowflakeCreateStageOperator(
    task_id='create_stage',
    stage=CSV_STAGE,
    file_format_name='standard_tsv',
    file_format_sql="""
        COMPRESSION = 'AUTO'
        FIELD_DELIMITER = '\\t'
        RECORD_DELIMITER = '\\n'
        ESCAPE = '\\\\'
        ESCAPE_UNENCLOSED_FIELD = NONE
    """,
    dag=main_dag,
)

for table_name in FBCachedDbApiHook.gen_postgres_tables():
    # While we've not yet signed a contract with Snowflake, remove 'users' table and the usual suspects to remove.
    if table_name in SCRAPE_TABLES_TO_SKIP or table_name == 'users':
        continue
    sub_dag = SubDagOperator(
        subdag=get_scrape_subdag(table_name),
        task_id=table_name,
        dag=main_dag,
    )
    sub_dag.set_upstream(create_stage)
