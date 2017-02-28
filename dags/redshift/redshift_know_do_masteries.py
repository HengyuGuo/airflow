from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
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
    'start_date': datetime(2017, 2, 15),
    'email': ['tpham@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_know_do_masteries',
    default_args=default_args,
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
)

wait_for_know_do_masteries = FBSignalSensor(
    task_id='wait_for_know_do_masteries',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='know_do_masteries',
    dag=dag,
)

insert_know_do_masteries = FBRedshiftOperator(
    task_id='insert_know_do_masteries',
    postgres_conn_id=REDSHIFT_CONN_ID,
    dag=dag,
    sql="""
    BEGIN;
    DROP TABLE IF EXISTS {output_schema}.know_do_masteries;
    
    CREATE TABLE IF NOT EXISTS {output_schema}.know_do_masteries (
        id numeric(10,0) NOT NULL,
        student_id numeric(10,0),
        know_do_id numeric(10,0),
        mastery character varying(255),
        reason character varying(255),
        created_at timestamp without time zone,
        updated_at timestamp without time zone,
        created_by numeric(10,0),
        updated_by numeric(10,0)
    ) 
    DISTKEY (student_id)
    SORTKEY (created_at, student_id, know_do_id);

    INSERT INTO {output_schema}.know_do_masteries (
        SELECT 
            id,
            student_id,
            know_do_id,
            mastery,
            reason,
            created_at,
            updated_at,
            created_by,
            updated_by
        FROM {input_schema}."know_do_masteries_{today}"
    );
    COMMIT;
    """.format(
        output_schema=DIM_AND_FCT_SCHEMA, 
        input_schema=STAGING_SCRAPES_SCHEMA,
        today='{{ ds }}'),
).set_upstream(wait_for_know_do_masteries)
