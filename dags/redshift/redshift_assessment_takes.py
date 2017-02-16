from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
    FBRedshiftOperator,
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
    'start_date': datetime(2017, 2, 15),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('redshift_assessment_takes', default_args=default_args, schedule_interval='@daily')

wait_for_assessment_takes = FBSignalSensor(
    task_id='wait_for_assessment_takes',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='assessment_takes',
    dag=dag,
)

insert_assessment_takes = FBRedshiftOperator(
    task_id='insert_assessment_takes',
    postgres_conn_id=REDSHIFT_CONN_ID,
    dag=dag,
    sql="""
    BEGIN;
    DROP TABLE IF EXISTS {output_schema}.assessment_takes;
    
    CREATE TABLE IF NOT EXISTS {output_schema}.assessment_takes (
        id NUMERIC(10,0),
        student_id NUMERIC(10,0),
        num_correct NUMERIC(10,0),
        created_at TIMESTAMP WITHOUT TIME ZONE,
        updated_at TIMESTAMP WITHOUT TIME ZONE,
        know_do_id NUMERIC(10,0),
        num_possible NUMERIC(10,0),
        taken_at TIMESTAMP WITHOUT TIME ZONE,
        approved_by_id NUMERIC(10,0),
        assessment_id NUMERIC(10,0),
        approved_time TIMESTAMP WITHOUT TIME ZONE,
        start_time TIMESTAMP WITHOUT TIME ZONE,
        is_content_assessment VARCHAR(5),
        created_by NUMERIC(10,0),
        updated_by NUMERIC(10,0),
        ell_locale VARCHAR(65535),
        objective_level_stats VARCHAR(65535),
        additional_time_given NUMERIC(10,0),
        academic_mindset_popup VARCHAR(5),
        student_signature VARCHAR(65535),
        time_zone VARCHAR(65535),
        ip_for_request VARCHAR(65535),
        ip_for_start VARCHAR(65535),
        ip_for_finish VARCHAR(65535),
        session_id_for_request VARCHAR(65535),
        session_id_for_start VARCHAR(65535),
        session_id_for_finish VARCHAR(65535),
        browser_id_for_request VARCHAR(65535),
        browser_id_for_start VARCHAR(65535),
        browser_id_for_finish VARCHAR(65535),
        invalidation_description VARCHAR(65535),
        visibility VARCHAR(256),
        invalidation_code VARCHAR(256),
        pcnt_to_pass NUMERIC(3,2),
        passed BOOLEAN
    )
    DISTKEY (student_id)
    SORTKEY (created_at, student_id, know_do_id);

    INSERT INTO {output_schema}.assessment_takes (
        SELECT
            ats.id,
            ats.student_id,
            ats.num_correct,
            ats.created_at,
            ats.updated_at,
            ats.know_do_id,
            ats.num_possible,
            ats.taken_at,
            ats.approved_by_id,
            ats.assessment_id,
            ats.approved_time,
            ats.start_time,
            ats.is_content_assessment,
            ats.created_by,
            ats.updated_by,
            ats.ell_locale,
            ats.objective_level_stats,
            ats.additional_time_given,
            ats.academic_mindset_popup,
            ats.student_signature,
            ats.time_zone,
            ats.ip_for_request,
            ats.ip_for_start,
            ats.ip_for_finish,
            ats.session_id_for_request,
            ats.session_id_for_start,
            ats.session_id_for_finish,
            ats.browser_id_for_request,
            ats.browser_id_for_start,
            ats.browser_id_for_finish,
            ats.invalidation_description,
            enum_name_for_value('visibility', ats.visibility, 'assessment_takes', 'AssessmentTake'),
            enum_name_for_value('invalidation_code', ats.invalidation_code, 'assessment_takes', 'AssessmentTake'),
            kd.pcnt_to_pass,
            (100 * ats.num_correct / cast(ats.num_possible as float)) >= cast(round(kd.pcnt_to_pass * 100, 0) as int)
        FROM {input_schema}."assessment_takes_{today}" ats
        LEFT JOIN {input_schema}."know_dos_{today}" kd
        ON (ats.know_do_id = kd.id)
    );
    COMMIT;
    """.format(
        output_schema=DIM_AND_FCT_SCHEMA,
        input_schema=STAGING_SCRAPES_SCHEMA,
        today='{{ ds }}',
    )
)
insert_assessment_takes.set_upstream(wait_for_assessment_takes)
