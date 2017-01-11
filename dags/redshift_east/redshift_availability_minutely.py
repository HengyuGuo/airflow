from airflow import DAG
from airflow.operators import FBRedshiftOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 12),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    # In case the warehouse is down, we don't want to dogpile
    # the airflow server with minute long jobs. Add an execution
    # timeout to prevent going over one minute.
    'execution_timeout': timedelta(seconds=30),
}

dag = DAG(
    'redshift_availability_minutely',
    default_args=default_args,
    schedule_interval='* * * * *',
)

# Note: We use a different user because
# the "airflow" user is marked with the etl_users GROUP
# and thus uses the etl queue. To track the availability
# of the normal query queue we use the "airflow_nonetl" user.

west_coast_availability_check = FBRedshiftOperator(
    task_id='west_coast_availability_check',
    sql="""-- Availability check query
        SELECT * FROM latest_scrape_sites LIMIT 5;
    """,
    postgres_conn_id='redshift_west_nonetl',
    dag=dag,
)

east_coast_availability_check = FBRedshiftOperator(
    task_id='east_coast_availability_check',
    sql="""-- Availability check query
        SELECT * FROM staging_scrapes.sites LIMIT 5;
    """,
    postgres_conn_id='redshift_east_nonetl',
    dag=dag,
)
