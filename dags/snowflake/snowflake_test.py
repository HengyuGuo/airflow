"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.jdbc_operator import JdbcOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 12, 8),
    'email': ['airflow@airflow.com'],
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
    'snowflake_test', default_args=default_args, schedule_interval='@daily')

t1 = JdbcOperator(
  task_id='count_table_rows',
  sql="""
      SELECT COUNT(1) FROM districts
  """,
  jdbc_conn_id='snowflake_test_db',
  dag=dag,
)
