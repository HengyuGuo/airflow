from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
    FBRedshiftOperator,
    FBHistoricalOperator,
)
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)

default_args = {
    'owner': 'kwerner',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 16),
    'email': ['kwerner@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('redshift_course_assignment_sections', default_args=default_args, schedule_interval='@daily')

wait_for_course_assignment_sections = FBSignalSensor(
    task_id='wait_for_course_assignment_sections',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='course_assignment_sections',
    dag=dag,
)

create_course_assignment_sections = FBRedshiftOperator(
    task_id='create_course_assignment_sections',
    sql="""
    BEGIN;
    CREATE TABLE IF NOT EXISTS {schema}.course_assignment_sections_historical (
         id NUMERIC(10,0), 
         course_assignment_id NUMERIC(10,0),
         section_id NUMERIC(10,0),
         created_at TIMESTAMP WITHOUT TIME ZONE,
         updated_at TIMESTAMP WITHOUT TIME ZONE,
         as_of DATE
    )
         DISTSTYLE KEY
         DISTKEY (course_assignment_id)
         SORTKEY (as_of
                 ,course_assignment_id
                 ,section_id
    );
    COMMIT;
    """.format(schema=DIM_AND_FCT_SCHEMA),
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
)

insert_course_assignment_sections = FBHistoricalOperator(
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='insert_course_assignment_sections',
    view_name='course_assignment_sections',
    select_sql="""
    SELECT
        id,
        course_assignment_id,
        section_id,
        created_at,
        updated_at,
        '{{ ds }}' as as_of
    FROM {{ params.input_schema }}.course_assignment_sections_{{ ds }} 
    """,
    dag=dag,
)

insert_course_assignment_sections.set_upstream([
    wait_for_course_assignment_sections,
    create_course_assignment_sections
])
