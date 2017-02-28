"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from constants import DEFAULT_SCHEDULE_INTERVAL
from redshift.constants import REDSHIFT_CONN_ID

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 12, 8),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'redshift_test',
    default_args=default_args,
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
)

t1 = PostgresOperator(
  task_id='count_table_rows',
  sql="""
      SELECT COUNT(1) FROM heroku_public.districts
  """,
  postgres_conn_id=REDSHIFT_CONN_ID,
  dag=dag,
)
