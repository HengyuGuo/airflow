from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
    FBHistoricalCheckOperator,
    FBHistoricalOperator,
)
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
    'start_date': datetime(2017, 2, 6),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_dim_courses',
    default_args=default_args,
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
)

wait_for_courses = FBSignalSensor(
    task_id='wait_for_courses',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='courses',
    dag=dag,
)

wait_for_subjects = FBSignalSensor(
    task_id='wait_for_subjects',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='subjects',
    dag=dag,
)

select_and_insert = FBHistoricalOperator(
    task_id='select_and_insert',
    view_name='dim_courses',
    select_sql="""
        SELECT
            c.id AS id,
            c.name AS name,
            c.grade_level AS grade_level,
            c.academic_year AS academic_year,
            enum_name_for_value('category', c.category, 'courses', 'Course') AS course_category,
            enum_name_for_value('visibility', c.visibility, 'courses', 'Course') AS visibility,
            CASE c.full_year_course WHEN 't' THEN TRUE WHEN 'f' THEN FALSE ELSE NULL END AS full_year_course,
            c.subject_id AS subject_id,
            s.name AS subject_name,
            CASE s.core WHEN 't' THEN TRUE WHEN 'f' THEN FALSE ELSE NULL END AS core_subject,
            enum_name_for_value('category', s.category, 'Subjects', 'Subject') AS subject_category,
            c.owner_id AS owner_id,
            c.owner_type AS owner_type,
            TO_DATE('{{ ds }}', 'YYYY-MM-DD') AS as_of
        FROM "{{ params.input_schema }}"."courses_{{ ds }}" c
        JOIN "{{ params.input_schema }}"."subjects_{{ ds }}" s
        ON (c.subject_id = s.id);
    """,
    dag=dag,
)
select_and_insert.set_upstream([wait_for_courses, wait_for_subjects])

check = FBHistoricalCheckOperator(
    task_id='check',
    conn_id=REDSHIFT_CONN_ID,
    table='{schema}.dim_courses_historical'.format(schema=DIM_AND_FCT_SCHEMA),
    dag=dag,
)
check.set_upstream(select_and_insert)
