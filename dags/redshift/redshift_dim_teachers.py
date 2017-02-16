from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
    FBRedshiftOperator,
    FBHistoricalOperator,
)
from datetime import datetime, timedelta
from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 11),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('redshift_dim_teachers', default_args=default_args, schedule_interval='@daily')

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

create_dim_teachers = FBRedshiftOperator(
    task_id='create_dim_teachers',
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="""
    BEGIN;
    CREATE TABLE IF NOT EXISTS {schema}.dim_teachers_historical (
        id integer,
        email character varying(1020),
        first_name character varying(1020),
        last_name character varying(1020),
        site_id integer,
        verified_teacher boolean,
        visibility character varying(256),
        site_name character varying(1020),
        district_id integer,
        site_min_grade_level integer,
        site_max_grade_level integer,
        nces_site_id integer,
        site_enrollment_group character varying(256),
        district_name character varying(256),
        nces_district_id integer,
        as_of date,
        created_at timestamp without time zone
    );
    COMMIT;
    """.format(schema=DIM_AND_FCT_SCHEMA),
    dag=dag,
)

insert_dim_teachers = FBHistoricalOperator(
    task_id='insert_dim_teachers',
    view_name='dim_teachers',
    select_sql="""
    SELECT
        u.id,
        u.email,
        u.first_name,
        u.last_name,
        u.default_site_id as site_id,
        CASE u.verified_teacher
            WHEN 't' THEN TRUE
            WHEN 'f' THEN FALSE
            ELSE NULL 
        END as verified_teacher,
        enum_name_for_value('visibility', u.visibility, 'users', 'Teacher') as visibility,
        s.name as site_name,
        s.district_id,
        s.min_grade_level as site_min_grade_level,
        s.max_grade_level as site_max_grade_level,
        s.nces_site_id,
        enum_name_for_value('enrollment_group', s.enrollment_group, 'sites', 'Site') as site_enrollment_group,
        d.name as district_name,
        d.nces_district_id,
        '{{ ds }}' as as_of,
        u.created_at
    FROM (
        SELECT *
        FROM {{ params.input_schema }}."users_{{ ds }}"
        WHERE type = 'Teacher'
    ) u
    LEFT JOIN {{ params.input_schema }}."sites_{{ ds }}" s
    ON u.default_site_id = s.id
    LEFT JOIN {{ params.input_schema }}."districts_{{ ds }}" d
    ON s.district_id = d.id
    """,
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
)

insert_dim_teachers.set_upstream([
    wait_for_users,
    wait_for_sites,
    wait_for_districts,
    create_dim_teachers,
])
