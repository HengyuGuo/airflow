from airflow import DAG
from airflow.operators import (
    FBRedshiftOperator,
    FBRedshiftToS3Transfer,
    FBSignalSensor,
    FBWriteSignalOperator,
)
from datetime import datetime, timedelta
from redshift_east.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2016, 12, 11),
    'email': ['astewart@summitps.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('redshift_dim_courses_historical', default_args=default_args, schedule_interval='@daily')

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

# TODO: Make this into a fully fledged operator after getting reviewed.
dim_transaction = FBRedshiftOperator(
    task_id='dim_transaction',
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="""
        BEGIN;

        CREATE TABLE IF NOT EXISTS {{ params.output_schema }}."dim_courses_historical" (
            id integer,
            name character varying(1020),
            grade_level integer,
            academic_year integer,
            course_category character varying(256),
            visibility character varying(256),
            full_year_course boolean,
            subject_id integer,
            subject_name character varying(256),
            core_subject boolean,
            subject_category character varying(256),
            owner_id integer,
            owner_type character varying(256),
            as_of character varying(10)
        )
        SORTKEY (as_of, id, academic_year);
        GRANT ALL PRIVILEGES ON {{ params.output_schema }}."dim_courses_historical" TO GROUP data_users;

        DELETE FROM {{ params.output_schema }}."dim_courses_historical" WHERE as_of = '{{ ds }}';

        INSERT INTO {{ params.output_schema }}."dim_courses_historical"
        SELECT
            c.id AS id,
            c.name AS name,
            c.grade_level AS grade_level,
            c.academic_year AS academic_year,
            enum_name_for_value('category', c.category, 'courses', 'Course') AS course_category,
            enum_name_for_value('visibility', c.visibility, 'courses', 'Course') AS visibility,
            c.full_year_course AS full_year_course,
            c.subject_id AS subject_id,
            s.name AS subject_name,
            s.core AS core_subject,
            enum_name_for_value('category', s.category, 'Subjects', 'Subject') AS subject_category,
            c.owner_id AS owner_id,
            c.owner_type AS owner_type,
            '{{ ds }}' AS as_of
        FROM {{ params.input_schema }}.courses c
        JOIN {{ params.input_schema }}.subjects s
        ON (c.subject_id = s.id);

        DROP VIEW IF EXISTS {{ params.output_schema }}."dim_courses";
        CREATE VIEW {{ params.output_schema }}."dim_courses" AS
            SELECT * FROM {{ params.output_schema }}."dim_courses_historical"
            WHERE as_of = '{{ ds }}';
        GRANT ALL PRIVILEGES ON {{ params.output_schema }}."dim_courses" TO GROUP data_users;

        COMMIT;
    """,
    params={
      'input_schema': STAGING_SCRAPES_SCHEMA,
      'output_schema': DIM_AND_FCT_SCHEMA,
    },
    dag=dag,
)
dim_transaction.set_upstream([wait_for_courses, wait_for_subjects])
