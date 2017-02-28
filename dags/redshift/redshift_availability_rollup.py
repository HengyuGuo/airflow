from airflow import DAG
from airflow.operators import (
    FBPostgresToS3JSONOperator,
    FBS3ToRedshiftOperator,
)
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from constants import (
    DEFAULT_SCHEDULE_INTERVAL,
    TOMORROW_MACRO,
)
from redshift.constants import REDSHIFT_CONN_ID

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 3),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_availability_rollup',
    default_args=default_args,
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
)

sql = """
    SELECT JSON_BUILD_OBJECT(
        'ds',
        '{today}',
        'id',
        job_id,
        'starttime',
        start_date,
        'endtime',
        end_date,
        'aborted',
        CASE state WHEN 'success' THEN 0 ELSE 1 END
    )
    FROM task_instance
    WHERE
        dag_id = 'redshift_availability_minutely' AND
        task_id = 'availability_check' AND
        start_date >= '{today}' AND
        end_date < '{tomorrow}'
""".format(
    today='{{ ds }}',
    tomorrow=TOMORROW_MACRO,
)

s3_key = '//plp-data-lake/import-staging/availability-check/east_coast/{{ ds }}.json.gz'

scrape = FBPostgresToS3JSONOperator(
    task_id='scrape',
    sql=sql,
    postgres_conn_id='airflow_store',
    s3_key=s3_key,
    dag=dag,
)

load = FBS3ToRedshiftOperator(
    task_id='load',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table='wild_west.availability_log',
    s3_key=s3_key,
    pre_sql="""
        -- TODO: Note that schema determination should replace this.
        -- But for now, we need a CREATE TABLE.
        CREATE TABLE IF NOT EXISTS wild_west.availability_log (
            ds date,
            id integer,
            starttime timestamp without time zone,
            endtime timestamp without time zone,
            aborted integer
        )
        SORTKEY (ds, id);

        DELETE FROM wild_west.availability_log
        WHERE ds = '{{ ds }}';
   """,
   is_json=True,
   dag=dag,
)
load.set_upstream(scrape)
