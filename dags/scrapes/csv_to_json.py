from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    FBCSVToJSONOperator,
    FBS3KeySensor,
)
from airflow.hooks import FBCachedDbApiHook

from datetime import date, datetime, timedelta
from redshift.constants import SLAVE_DB_CONN_ID

default_args = {
    'owner': 'ilan',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 1),
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

# If we use this query often enough, it may be worth it to store these results as a file in S3
def get_table_names():
    hook = FBCachedDbApiHook(conn_id=SLAVE_DB_CONN_ID)
    key = PARENT_DAG_NAME + '_get_table_names_' + str(date.today())
    records = hook.get_records(
        key=key,
        sql="""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
              AND table_name NOT IN (
                'google_tokens',
                'schema_migrations',
                'data_imports',
                'versions'
              )
            ORDER BY table_name
        """,
    )
    return [x[0] for x in records]

def get_csv_to_json_subdag(table_name):
    dag = DAG(
        '{}.{}'.format(PARENT_DAG_NAME, table_name),
        default_args=default_args,
        schedule_interval=SCHEDULE_INTERVAL,
    )

    data_s3_key = '//plp-data-lake/import-staging/csv-scrapes/{}/{}.tsv.gz'.format(
        table_name,
        '{{ ds }}',
    )

    schema_s3_key = '//plp-data-lake/import-staging/csv-scrapes/{}/schemata/{}.json'.format(
        table_name,
        '{{ ds }}',
    )

    json_s3_key = '//plp-data-lake/import-staging/csv-scrapes/{}/{}.json.gz'.format(
        table_name,
        '{{ ds }}',
    )

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

table_names = get_table_names()
for table_name in table_names:
    SubDagOperator(
        subdag=get_csv_to_json_subdag(table_name),
        task_id=table_name,
        dag=main_dag,
    )
