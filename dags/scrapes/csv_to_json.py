from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    FBCSVToJSONOperator,
    FBS3KeySensor,
)
from airflow.hooks import FBCachedDbApiHook

from datetime import date, datetime, timedelta
from scrapes.constants import SCRAPE_TABLES_TO_SKIP
from scrapes.utils import (
    get_data_s3_key,
    get_schema_s3_key,
    get_json_s3_key,
)
from redshift.constants import SLAVE_DB_CONN_ID

default_args = {
    'owner': 'ilan',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 27),
    'email': ['igoodman@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PARENT_DAG_NAME = 'csv_to_json'
SCHEDULE_INTERVAL = '0 10 * * *' # Start at 10AM each day to not waste CPU

main_dag = DAG(
    PARENT_DAG_NAME,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
)

def get_csv_to_json_subdag(table_name):
    dag = DAG(
        '{}.{}'.format(PARENT_DAG_NAME, table_name),
        default_args=default_args,
        schedule_interval=SCHEDULE_INTERVAL,
    )

    data_s3_key = get_data_s3_key(table_name)
    schema_s3_key = get_schema_s3_key(table_name)
    json_s3_key = get_json_s3_key(table_name)

    wait_for_data = FBS3KeySensor(
        task_id='wait_for_data',
        s3_key=data_s3_key,
        dag=dag,
    )
    wait_for_schema = FBS3KeySensor(
        task_id='wait_for_schema',
        s3_key=schema_s3_key,
        dag=dag,
    )

    convert_to_json = FBCSVToJSONOperator(
        task_id='convert_to_json',
        data_s3_key=data_s3_key,
        schema_s3_key=schema_s3_key,
        json_s3_key=json_s3_key,
        dag=dag,
    )
    convert_to_json.set_upstream([wait_for_data, wait_for_schema])

    return dag

for table_name in FBCachedDbApiHook.gen_postgres_tables():
    if table_name in SCRAPE_TABLES_TO_SKIP:
        continue
    SubDagOperator(
        subdag=get_csv_to_json_subdag(table_name),
        task_id=table_name,
        dag=main_dag,
    )
