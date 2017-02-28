"""
This file adds default permissions to the warehouse. New users should:
1) add themselves to data_eng_role or analyst_role in ROLES_TO_USERS
    (dags/snowflake/constants.py)
2) run these commands:
    airflow test snowflake_set_role_permissions configure_users 2016-01-01
    airflow test snowflake_set_role_permissions grant_roles 2016-01-01
    airflow test snowflake_set_role_permissions grant_databases 2016-01-01
    airflow test snowflake_set_role_permissions grant_warehouses 2016-01-01
    airflow test snowflake_set_role_permissions grant_schemas 2016-01-01
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
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'snowflake_set_role_permissions',
    default_args=default_args,
    schedule_interval='@once',
)

user_list = set()
for users in ROLES_TO_USERS.values():
    user_list.update(users)

DATABASE_NAME = 'main'  # There can only be one!

DEFAULT_WAREHOUSE = 'user_queries'
WAREHOUSES = [
    DEFAULT_WAREHOUSE,
]

def flatten(lists):
    return reduce(lambda x, y: x + y, lists)

# Configure users
def configure_user_sql(user):
    best_role = None
    for role, user_list in ROLES_TO_USERS.iteritems():
        if user in user_list:
            best_role = role
    return """
        ALTER USER {user} SET
        DEFAULT_ROLE = '{role}'
        DEFAULT_WAREHOUSE = '{warehouse}';
    """.format(
        user=user,
        role=best_role.upper(),
        warehouse=DEFAULT_WAREHOUSE.upper(),
    )

configure_users = FBSnowflakeOperator(
    task_id='configure_users',
    snowflake_conn_id=SNOWFLAKE_ADMIN_CONN_ID,
    sql=['USE ROLE ACCOUNTADMIN;'] + [configure_user_sql(user) for user in user_list],
    dag=dag,
)

# Grant roles
def grant_role_sqls(user):
    ret = []
    for role, user_list in ROLES_TO_USERS.iteritems():
        if user in user_list:
            ret.append('GRANT ROLE {} TO USER {};'.format(role, user))
    return ret

grant_roles = FBSnowflakeOperator(
    task_id='grant_roles',
    snowflake_conn_id=SNOWFLAKE_ADMIN_CONN_ID,
    sql=flatten([grant_role_sqls(user) for user in user_list]),
    dag=dag,
)

# Grant databases
def grant_database_sql(role):
    return 'GRANT ALL ON DATABASE {} TO {};'.format(DATABASE_NAME, role)

grant_databases = FBSnowflakeOperator(
    task_id='grant_databases',
    snowflake_conn_id=SNOWFLAKE_ADMIN_CONN_ID,
    sql=[grant_database_sql(role) for role in ROLES_TO_USERS],
    dag=dag,
)

# Grant warehouses
def grant_warehouse_sqls(role):
    return ['GRANT ALL ON WAREHOUSE {} TO {};'.format(warehouse, role) for warehouse in WAREHOUSES]

grant_warehouses = FBSnowflakeOperator(
    task_id='grant_warehouses',
    snowflake_conn_id=SNOWFLAKE_ADMIN_CONN_ID,
    sql=flatten([grant_warehouse_sqls(role) for role in ROLES_TO_USERS]),
    dag=dag,
)

# Grant schemas
def grant_schema_sqls(role):
    return ['GRANT ALL ON SCHEMA {} TO {};'.format(schema, role) for schema in SCHEMAS]

grant_schemas = FBSnowflakeOperator(
    task_id='grant_schemas',
    snowflake_conn_id=SNOWFLAKE_ADMIN_CONN_ID,
    sql=flatten([grant_schema_sqls(role) for role in ROLES_TO_USERS]),
    dag=dag,
)
