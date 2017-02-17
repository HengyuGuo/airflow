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
    'owner': 'tpham',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 21),
    'email': ['tpham@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('redshift_course_assignments', default_args=default_args, schedule_interval='@daily')

wait_for_course_assignments = FBSignalSensor(
    task_id='wait_for_course_assignments',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='course_assignments',
    dag=dag,
)

create_course_assignments = FBRedshiftOperator(
    task_id='create_course_assignments',
    sql="""
    BEGIN;
    CREATE TABLE IF NOT EXISTS {schema}.course_assignments_historical (
        id integer NOT NULL,
        course_id integer,
        student_id integer,
        created_at timestamp without time zone,
        updated_at timestamp without time zone,
        project_score integer,
        raw_cog_skill_score double precision,
        cog_skill_pcnt integer,
        num_projects_graded integer,
        num_projects_overdue integer,
        num_projects_turned_in integer,
        num_projects_total integer,
        num_projects_ungraded integer,
        num_projects_exempted integer,
        manually_graded boolean,
        overall_score integer,
        letter_grade character varying(1020),
        power_num_mastered integer,
        power_out_of integer,
        power_expected_pcnt integer,
        power_on_track boolean,
        addl_num_mastered integer,
        addl_out_of integer,
        addl_expected_pcnt integer,
        power_expected double precision,
        addl_expected double precision,
        target_letter_grade character varying(1020),
        power_num_behind integer,
        overall_score_changed_at timestamp without time zone,
        start_date date,
        end_date date,
        raw_concept_score double precision,
        concept_pcnt integer,
        site_id integer,
        visibility character varying(256),
        as_of date
    );
    COMMIT;
    """.format(schema=DIM_AND_FCT_SCHEMA),
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
)

insert_course_assignments = FBHistoricalOperator(
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='insert_course_assignments',
    view_name='course_assignments',
    select_sql="""
    SELECT
        id,
        course_id,
        student_id,
        created_at,
        updated_at,
        project_score,
        CAST(raw_cog_skill_score as numeric(4,2)) as raw_cog_skill_score,
        cog_skill_pcnt,
        num_projects_graded,
        num_projects_overdue,
        num_projects_turned_in,
        num_projects_total,
        num_projects_ungraded,
        num_projects_exempted,
        CASE manually_graded
            WHEN 't' THEN True
            WHEN 'f' THEN False
            ELSE NULL
        END as manually_graded,
        overall_score,
        letter_grade,
        power_num_mastered,
        power_out_of,
        power_expected_pcnt,
        CASE power_on_track
            WHEN 't' THEN True
            WHEN 'f' THEN False
            ELSE NULL
        END as power_on_track,
        addl_num_mastered,
        addl_out_of,
        addl_expected_pcnt,
        CAST(power_expected as numeric(4,2)) as power_expected,
        CAST(power_expected as numeric(4,2)) as addl_expected,
        target_letter_grade,
        power_num_behind,
        overall_score_changed_at,
        start_date,
        end_date,
        CAST(raw_concept_score as numeric(4,2)) as raw_concept_score,
        concept_pcnt,
        site_id,
        enum_name_for_value('visibility', visibility, 'course_assignments', 'CourseAssignment') as visibility,
        '{{ ds }}' as as_of
    FROM {{ params.input_schema }}."course_assignments_{{ ds }}"
    """,
    dag=dag,
)

insert_course_assignments.set_upstream([
    wait_for_course_assignments, 
    create_course_assignments,
])
