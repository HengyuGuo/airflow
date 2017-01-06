from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
)
from datetime import datetime, timedelta
from redshift_east.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2016, 12, 11),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('redshift_dim_teachers_historical', default_args=default_args, schedule_interval='@daily')

wait_for_users = FBSignalSensor(
    task_id='wait_for_users',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='users',
    dag=dag,
)

wait_for_sites = FBSignalSensor(
    task_id='wait_for_sites',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='sites',
    dag=dag,
)

wait_for_districts = FBSignalSensor(
    task_id='wait_for_districts',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='districts',
    dag=dag,
)
