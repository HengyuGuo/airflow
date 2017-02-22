from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    FBPostgresToS3CSVOperator,
    FBS3ToRedshiftOperator,
)
from airflow.hooks import FBCachedDbApiHook

from datetime import datetime, timedelta
from scrapes.constants import SCRAPE_TABLES_TO_SKIP
from scrapes.utils import (
    get_data_s3_key,
    get_schema_s3_key
)
from redshift.constants import (
    HEROKU_PUBLIC_SCHEMA,
    REDSHIFT_CONN_ID,
    SLAVE_DB_CONN_ID,
)

default_args = {
    'owner': 'ilan',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 7),
    'email': ['igoodman@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PARENT_DAG_NAME = 'redshift_csv_scrapes'
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

    copy_to_s3_transaction = FBPostgresToS3CSVOperator(
        task_id='copy_to_s3_transaction',
        postgres_conn_id=SLAVE_DB_CONN_ID,
        table_name=table_name,
        data_s3_key=data_s3_key,
        schema_s3_key=schema_s3_key,
        dag=dag
    )

    upload_table = FBS3ToRedshiftOperator(
        task_id='upload_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table='{schema}.{table_name}'.format(
            schema=HEROKU_PUBLIC_SCHEMA,
            table_name=table_name,
        ),
        s3_key=data_s3_key,
        is_json=False,
        drop_and_create=True,
        schema_s3_key=schema_s3_key,
        dag=dag,
    )
    # TODO: will need to add a Sensor object when we ultimately separate these
    upload_table.set_upstream(copy_to_s3_transaction)

    return dag

for table_name in FBCachedDbApiHook.gen_postgres_tables():
    if table_name in SCRAPE_TABLES_TO_SKIP:
        continue
    SubDagOperator(
        subdag=get_scrape_subdag(table_name),
        task_id=table_name,
        dag=main_dag
    )
