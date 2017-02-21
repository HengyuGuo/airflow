from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
    FBHistoricalOperator,
    FBRedshiftOperator,
)
from datetime import datetime, timedelta
from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)

default_args = {
    'owner': 'ilan',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 21),
    'email': ['igoodman@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('redshift_dim_parents', default_args=default_args, schedule_interval='@daily')

wait_for_users = FBSignalSensor(
    task_id='wait_for_users',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='users',
    dag=dag,
)

# Bigint instead of integer in preparation for using larger IDs
create_dim_parents = FBRedshiftOperator(
    task_id='create_dim_parents',
    sql=[
        "BEGIN;",
        """
        CREATE TABLE IF NOT EXISTS {schema}.dim_parents_historical (
            id bigint NOT NULL,
            email character varying(1024),
            first_name character varying(1024),
            last_name character varying(1024),
            as_of date NOT NULL
        )
        DISTKEY (id)
        SORTKEY (as_of, id);
        """.format(schema=DIM_AND_FCT_SCHEMA),
        "COMMIT;",
    ],
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
)

insert_dim_parents = FBHistoricalOperator(
    task_id='insert_dim_parents',
    view_name='dim_parents',
    select_sql="""
        SELECT
            id,
            email,
            first_name,
            last_name,
            '{{ ds }}' as as_of
        FROM {{ params.input_schema }}."users_{{ ds }}"
        WHERE type = 'Parent'
    """,
    dag=dag,
)
insert_dim_parents.set_upstream([
    wait_for_users,
    create_dim_parents,
])
