from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
    FBHistoricalOperator,
    FBRedshiftOperator,
)
from datetime import datetime, timedelta
from constants import DEFAULT_SCHEDULE_INTERVAL
from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)

default_args = {
    'owner': 'tpham',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 17),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_dim_students',
    default_args=default_args,
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
)

wait_for_users = FBSignalSensor(
    task_id='wait_for_users',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='users',
    dag=dag,
)

wait_for_sites = FBSignalSensor(
    task_id='wait_for_sites',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='sites',
    dag=dag,
)

wait_for_districts = FBSignalSensor(
    task_id='wait_for_districts',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='districts',
    dag=dag,
)

create_dim_students = FBRedshiftOperator(
    task_id='create_dim_students',
    sql="""
    BEGIN;
    CREATE TABLE IF NOT EXISTS {schema}.dim_students_historical (
        id integer NOT NULL,
        email character varying(1020),
        school_id character varying(256),
        first_name character varying(1020),
        last_name character varying(1020),
        site_id integer,
        mentor_id integer,
        grade_level integer,
        visibility character varying(256),
        content_form_status character varying(256),
        currently_enrolled boolean,
        site_name character varying(1020),
        district_id integer,
        site_max_grade_level integer,
        site_min_grade_level integer,
        nces_site_id integer,
        site_enrollment_group character varying(256),
        district_name character varying(256),
        nces_district_id integer,
        as_of date,
        current_gpa numeric(16,12),
        state_student_id character varying(65535)
    ) 
    DISTKEY (id)
    SORTKEY (as_of, id, site_id);
    COMMIT;
    """.format(schema=DIM_AND_FCT_SCHEMA),
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
)

insert_dim_students = FBHistoricalOperator(
    task_id='insert_dim_students',
    view_name='dim_students',
    select_sql="""
    SELECT
        u.id,
        u.email,
        u.school_id,
        u.first_name,
        u.last_name,
        u.site_id,
        u.mentor_id,
        u.grade_level,
        enum_name_for_value('visibility', u.visibility, 'users', 'Student') as visibility,
        enum_name_for_value('consent_form_status', u.consent_form_status, 'users', 'Student') as content_form_status,
        CASE u.currently_enrolled
            WHEN 't' THEN TRUE 
            WHEN 'f' THEN FALSE 
            ELSE NULL 
        END as currently_enrolled,
        s.name as site_name,
        s.district_id, 
        s.min_grade_level as site_min_grade_level,
        s.max_grade_level as site_max_grade_level,
        s.nces_site_id,
        enum_name_for_value('enrollment_group', s.enrollment_group, 'sites', 'Site') as site_enrollment_group,
        d.name as district_name,
        d.nces_district_id,
        '{{ ds }}' as as_of,
        u.current_gpa,
        u.state_id as state_student_id 
    FROM (
        SELECT * 
        FROM {{ params.input_schema }}."users_{{ ds }}"
        WHERE type = 'Student'
    ) u 
    LEFT JOIN {{ params.input_schema }}."sites_{{ ds }}" s
    ON s.id = u.site_id
    LEFT JOIN {{ params.input_schema }}."districts_{{ ds }}" d
    ON d.id = s.district_id
    """,
    dag=dag,
)
insert_dim_students.set_upstream([
    wait_for_districts,
    wait_for_users,
    wait_for_sites,
    create_dim_students,
])
