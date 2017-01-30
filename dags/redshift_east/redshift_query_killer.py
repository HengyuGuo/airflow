from airflow import DAG
from airflow.operators import (
    FBRedshiftQueryKillerOperator,
    FBS3ToRedshiftOperator,
)
from datetime import datetime, timedelta
from redshift_east.constants import REDSHIFT_ADMIN_CONN_ID

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 23),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'execution_timeout': timedelta(minutes=3),
}

dag = DAG(
    'redshift_query_killer',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
)

s3_key = '//plp-data-lake/import-staging/query_killer/{0}_{1}.json.gz'.format(
    REDSHIFT_ADMIN_CONN_ID,
    '{{ ts }}',
)
query_killer = FBRedshiftQueryKillerOperator(
    task_id='query_killer',
    redshift_conn_id=REDSHIFT_ADMIN_CONN_ID,
    log_s3_key=s3_key,
    dag=dag,
)
upload_log = FBS3ToRedshiftOperator(
    task_id='upload_log',
    redshift_conn_id=REDSHIFT_ADMIN_CONN_ID,
    table='wild_west.query_killer_log',
    s3_key=s3_key,
    pre_sql="""
        CREATE TABLE IF NOT EXISTS wild_west.query_killer_log (
            killer_run_time timestamp without time zone,
            too_long boolean,
            query_id integer,
            "user" character varying(128),
            start_time timestamp without time zone,
            minutes_running integer,
            minutes_queued integer,
            sql character varying(65535)
        )
        SORTKEY (killer_run_time, query_id);
    """,
    is_json=True,
    dag=dag,
)
upload_log.set_upstream(query_killer)
