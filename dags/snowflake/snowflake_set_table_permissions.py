"""
Snowflake is adding default permissions to their implementation.

Until that is launched, we have an annoying situation:
1) Run airflow task to generate a new table.
2) Don't have the permissions in Mode to see the table.

Our best solution right now is to keep running the grants continuously.
"""
from airflow import DAG
from airflow.operators import FBSnowflakeOperator
from datetime import datetime, timedelta
from snowflake.constants import (
    SNOWFLAKE_ADMIN_CONN_ID,
    ROLES_TO_USERS,
    SCHEMAS,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'email': ['astewart@summitps.org'],
    # Turning off failure emails until we can batch failures.
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'snowflake_set_table_permissions',
    default_args=default_args,
    schedule_interval='@once',
)

sqls = []
for role in ROLES_TO_USERS:
    for schema in SCHEMAS:
        sqls.append('GRANT ALL ON ALL TABLES IN SCHEMA {} TO {};'.format(schema, role))
        sqls.append('GRANT ALL ON ALL VIEWS IN SCHEMA {} TO {};'.format(schema, role))

grant_tables = FBSnowflakeOperator(
    task_id='grant_tables',
    snowflake_conn_id=SNOWFLAKE_ADMIN_CONN_ID,
    sql=sqls,
    dag=dag,
)
