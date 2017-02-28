from airflow import DAG
from airflow.operators import (
    FBHistoricalOperator,
    FBSignalSensor,
    FBRedshiftOperator,
)
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from constants import DEFAULT_SCHEDULE_INTERVAL
from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 15),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_section_teachers',
    default_args=default_args,
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
)

wait_for_section_teachers = FBSignalSensor(
    task_id='wait_for_section_teachers',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='section_teachers',
    dag=dag,
)

create_section_teachers_historical = FBRedshiftOperator(
    task_id='create_section_teachers_historical',
    sql="""
    BEGIN;

    CREATE TABLE IF NOT EXISTS {schema}.section_teachers_historical (
        id NUMERIC(10,0),
        section_id NUMERIC(10,0),
        teacher_id NUMERIC(10,0),
        created_at TIMESTAMP WITHOUT TIME ZONE,
        updated_at TIMESTAMP WITHOUT TIME ZONE,
        visibility VARCHAR(256),
        as_of DATE
    )
    DISTKEY (teacher_id)
    SORTKEY (as_of, teacher_id, section_id, visibility);

    COMMIT;
    """.format(schema=DIM_AND_FCT_SCHEMA),
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
)

select_and_insert = FBHistoricalOperator(
    task_id='select_and_insert',
    view_name='section_teachers',
    select_sql="""
        SELECT
            id,
            section_id,
            teacher_id,
            created_at,
            updated_at,
            enum_name_for_value('visibility', visibility, 'section_teachers', 'SectionTeacher'),
            '{{ ds }}' AS as_of
        FROM {{ params.input_schema }}."section_teachers_{{ ds }}"
    """,
    dag=dag,
)
select_and_insert.set_upstream([wait_for_section_teachers, create_section_teachers_historical])
