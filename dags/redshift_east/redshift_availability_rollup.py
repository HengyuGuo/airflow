from airflow import DAG
from airflow.operators import FBRedshiftOperator
from datetime import datetime, timedelta
from redshift_east.constants import REDSHIFT_CONN_ID

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 13),
    'email': ['astewart@summitps.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_availability_rollup',
    default_args=default_args,
    schedule_interval='@daily',
)

sql = """
    BEGIN;

    CREATE TABLE IF NOT EXISTS wild_west.availability_log (
        ds date,
        id integer,
        starttime timestamp without time zone,
        endtime timestamp without time zone,
        aborted integer
    )
    SORTKEY (ds, id);

    DELETE FROM wild_west.availability_log
    WHERE ds = '{yesterday}';

    INSERT INTO wild_west.availability_log
    SELECT
        '{yesterday}' AS ds,
        query AS id,
        starttime,
        endtime,
        aborted
    FROM stl_query
    WHERE
        userid = {userid} AND
        querytxt LIKE '-- Availability check query%' AND
        starttime >= '{yesterday}' AND
        endtime < '{today}';

    COMMIT;
""".format(
    yesterday='{{ macros.ds_add(ds, -1) }}',
    userid='{{ params.userid }}',
    today='{{ ds }}',
)

west_coast = FBRedshiftOperator(
    task_id='west_coast',
    sql=sql,
    params={'userid': 128}, # user: airflow_nonetl
    postgres_conn_id='redshift_west_nonetl',
    dag=dag,
)

east_coast = FBRedshiftOperator(
    task_id='east_coast',
    sql=sql,
    params={'userid': 120}, # user: airflow_nonetl
    postgres_conn_id='redshift_east_nonetl',
    dag=dag,
)
