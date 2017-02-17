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
    'start_date': datetime(2017, 2, 16),
    'email': ['tpham@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('redshift_dim_sections', default_args=default_args, schedule_interval='@daily')

wait_for_sections = FBSignalSensor(
    task_id='wait_for_sections',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='sections',
    dag=dag,
)

create_dim_sections = FBRedshiftOperator(
    task_id='create_dim_sections',
    sql="""
    BEGIN;
    CREATE TABLE IF NOT EXISTS {schema}.dim_sections_historical (
        id numeric(10,0) NOT NULL,
        sis_id character varying(65535),
        name character varying(255),
        clever_id character varying(65535),
        site_id numeric(10,0),
        create_at timestamp without time zone,
        updated_at timestamp without time zone,
        sis_course_id numeric(10,0),
        academic_year numeric(10,0),
        create_source character varying(256),
        as_of date
    );
    COMMIT;
    """.format(schema=DIM_AND_FCT_SCHEMA),
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
)

insert_dim_sections = FBHistoricalOperator(
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='insert_dim_sections',
    view_name='dim_sections',
    select_sql="""
    SELECT
        s.id,
        s.sis_id,
        s.name,
        s.clever_id,
        s.site_id,
        s.created_at,
        s.updated_at,
        s.sis_course_id,
        s.academic_year,
        enum_name_for_value('create_source', s.create_source, 'sections', 'Section') as create_source,
        '{{ ds }}' as as_of
    FROM {{ params.input_schema }}."sections_{{ ds }}" s
    """,
    dag=dag,
)

insert_dim_sections.set_upstream([
    wait_for_sections, 
    create_dim_sections
])
