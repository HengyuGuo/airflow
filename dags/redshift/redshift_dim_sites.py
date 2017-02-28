from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
    FBRedshiftOperator,
    FBHistoricalOperator,
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
    'start_date': datetime(2017, 2, 16),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_dim_sites',
    default_args=default_args,
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
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

create_dim_sites = FBRedshiftOperator(
    task_id='create_dim_sites',
    sql="""
    BEGIN;
    CREATE TABLE IF NOT EXISTS {schema}.dim_sites_historical (
        id numeric(10,0) NOT NULL,
        nces_site_id numeric(10,0),
        name character varying(255),
        enrollment_group character varying(256),
        district_id numeric(10,0),
        nces_district_id numeric(10,0),
        district_name character varying(65535),
        as_of date,
        status character varying(256),
        school_type character varying(65535),
        student_data_sharing character varying(256),
        teacher_sync_enabled bool,
        should_sync_clever bool,
        student_sync_enabled bool,
        section_sync_enabled bool
    );
    COMMIT;
    """.format(schema=DIM_AND_FCT_SCHEMA),
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
)

insert_dim_sites = FBHistoricalOperator(
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='insert_dim_sites',
    view_name='dim_sites',
    select_sql="""
    SELECT
        s.id,
        s.nces_site_id,
        s.name,
        public.enum_name_for_value('enrollment_group', s.enrollment_group, 'sites', 'Site') as enrollment_group,
        s.district_id,
        d.nces_district_id,
        d.name as district_name,
        '{{ ds }}' as as_of,
        enum_name_for_value('status', status, 'sites', 'Site') as status,
        CASE 
            WHEN summit.site_id IS NOT NULL
            THEN 'SPS'
            WHEN slp.site_id IS NOT NULL 
                AND public.enum_name_for_value('enrollment_group', s.enrollment_group, 'sites', 'Site') IN ('glt_cohort_1', 'glt_cohort_2')
            THEN 'SLP'
            WHEN public.enum_name_for_value('enrollment_group', s.enrollment_group, 'sites', 'Site') like '%d2t%'
            THEN 'IT'
            ELSE null 
        END as school_type,
        enum_name_for_value('student_data_sharing', student_data_sharing, 'sites', 'Site') as student_data_sharing,
        CASE scs.teacher_sync_enabled
            WHEN 't' THEN true
            WHEN 'f' THEN false
            ELSE NULL
        END AS teacher_sync_enabled,
        CASE scs.should_sync_clever
            WHEN 't' THEN true
            WHEN 'f' THEN false
            ELSE NULL
        END AS should_sync_clever,
        CASE scs.student_sync_enabled
            WHEN 't' THEN true
            WHEN 'f' THEN false
            ELSE NULL
        END AS student_sync_enabled,
        CASE scs.section_sync_enabled
            WHEN 't' THEN true
            WHEN 'f' THEN false
            ELSE NULL
        END AS section_sync_enabled
    FROM {{ params.input_schema }}."sites_{{ ds }}" s
    LEFT JOIN {{ params.input_schema}}."districts_{{ ds }}" d
    ON s.district_id = d.id
    LEFT JOIN public.summit_site_info summit
    ON summit.site_id = s.id
    LEFT JOIN wild_west.slp_site_info slp
    ON slp.site_id = s.id
    LEFT JOIN {{ params.input_schema }}."site_clever_settings_{{ ds }}" scs
    ON s.id = scs.site_id
    """,
    dag=dag,
)

insert_dim_sites.set_upstream([
    wait_for_sites, 
    wait_for_districts,
    create_dim_sites,
])
