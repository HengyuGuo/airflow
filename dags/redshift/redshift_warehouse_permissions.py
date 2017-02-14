"""
This file adds default permissions to the warehouse. New users should:
1) add themselves to data_eng or analysts group
2) run these commands:
    airflow test redshift_set_warehouse_permissions add_members 2016-01-01
    airflow test redshift_set_warehouse_permissions permissions 2016-01-01
"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    FBRedshiftOperator,
    FBRedshiftToS3Transfer,
    FBWriteSignalOperator,
)

from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from redshift.constants import (
    HEROKU_PUBLIC_SCHEMA,
    REDSHIFT_ADMIN_CONN_ID,
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    STAGING_SCRAPES_WRITE_SCHEMA,
)

default_args = {
    'owner': 'keoki',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'email': ['kseu@summitps.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_set_warehouse_permissions',
    default_args=default_args,
    schedule_interval='@once',
)

# permissions
ALL = "ALL"
DELETE = "DELETE"
EXECUTE = "EXECUTE"
INSERT = "INSERT"
SELECT = "SELECT"
UPDATE = "UPDATE"

# the defines the users who we want to be added to the groups. Key is group,
# value is list of users
permission_groups = {
    'analysts': ['ashah', 'keoki', 'ilan', 'palasha', 'hcli', 'quicksight', 'tpham'],
    'data_eng': [
        'astewart',
        'hcli',
        'ilan',
        'keoki',
        'kwerner',
        'palasha',
        'tpham',
    ],
    'etl_tools': ['matillion', 'treasure_data', 'airflow', 'alooma', 'airflow_admin'],
    'query_tools': ['mode', 'tableau', 'quicksight'],
    'workflow_tools': ['matillion', 'treasure_data', 'airflow', 'airflow_admin'],
}

user_list = set()
for users in permission_groups.values():
    user_list.update(users)


DEFAULT_PERMS = ALL
schemas = [
    'airflow',
    'asana',
    HEROKU_PUBLIC_SCHEMA,
    'heroku_public',
    'matillion',
    'public',
    STAGING_SCRAPES_SCHEMA,
    STAGING_SCRAPES_WRITE_SCHEMA,
    'wild_west',
    'zendesk',
]

add_ddl = []
for group, users in permission_groups.items():
    for user in users:
        add_ddl.append("ALTER GROUP {group_name} ADD USER {user};".format(
                user=user,
                group_name=group,
            )
        )
add_members = FBRedshiftOperator(
    task_id='add_members',
    postgres_conn_id=REDSHIFT_ADMIN_CONN_ID,
    sql="\n".join(add_ddl),
    dag=dag,
)

perms_sql = []
for schema in schemas:
    for user in user_list:
        perms_sql.append(
            "GRANT ALL ON SCHEMA {schema} TO {user};".format(
                schema=schema,
                user=user,
            )
        )
        for target in ('TABLES', 'FUNCTIONS'):
            perms_sql.append("""
                    GRANT {perms} ON ALL TABLES IN SCHEMA {schema} TO {user};
                """.format(
                    perms=DEFAULT_PERMS,
                    schema=schema,
                    user=user,
                ),
            )

            perms_sql.append(
                """ ALTER DEFAULT PRIVILEGES FOR USER {user}
                    IN SCHEMA {schema} GRANT {perms} ON {target} TO PUBLIC;
                """.format(
                    perms=DEFAULT_PERMS,
                    schema=schema,
                    target=target,
                    user=user,
                ),
            )

permissions = FBRedshiftOperator(
    task_id='permissions',
    postgres_conn_id=REDSHIFT_ADMIN_CONN_ID,
    sql="\n".join(perms_sql),
    dag=dag,
)
permissions.set_upstream(add_members)
