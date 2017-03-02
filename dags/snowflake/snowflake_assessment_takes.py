"""
TODO: Need to evaluate enums visibility, invalidation_code
"""
from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
    FBSnowflakeOperator,
)
from datetime import datetime, timedelta
from constants import DEFAULT_SCHEDULE_INTERVAL
from snowflake.constants import (
    SNOWFLAKE_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    PUBLIC_SCHEMA,
    DS_FOR_TABLE,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2017, 3, 1, 3),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'snowflake_assessment_takes',
    default_args=default_args,
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
)

wait_for_assessment_takes = FBSignalSensor(
    task_id='wait_for_assessment_takes',
    conn_id=SNOWFLAKE_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='assessment_takes',
    dag=dag,
)

wait_for_know_dos = FBSignalSensor(
    task_id='wait_for_know_dos',
    conn_id=SNOWFLAKE_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='know_dos',
    dag=dag,
)

insert_assessment_takes = FBSnowflakeOperator(
    task_id='insert_assessment_takes',
    dag=dag,
    sql=[
        'BEGIN;',
        'DROP TABLE IF EXISTS {output_schema}.assessment_takes;'.format(output_schema=PUBLIC_SCHEMA),
        """
        CREATE TABLE IF NOT EXISTS {output_schema}.assessment_takes (
            id INTEGER,
            student_id INTEGER,
            num_correct INTEGER,
            created_at TIMESTAMP WITHOUT TIME ZONE,
            updated_at TIMESTAMP WITHOUT TIME ZONE,
            know_do_id INTEGER,
            num_possible INTEGER,
            taken_at TIMESTAMP WITHOUT TIME ZONE,
            approved_by_id INTEGER,
            assessment_id INTEGER,
            approved_time TIMESTAMP WITHOUT TIME ZONE,
            start_time TIMESTAMP WITHOUT TIME ZONE,
            is_content_assessment BOOLEAN,
            created_by INTEGER,
            updated_by INTEGER,
            ell_locale VARCHAR,
            objective_level_stats VARCHAR,
            additional_time_given INTEGER,
            academic_mindset_popup VARCHAR,
            student_signature VARCHAR,
            time_zone VARCHAR,
            ip_for_request VARCHAR,
            ip_for_start VARCHAR,
            ip_for_finish VARCHAR,
            session_id_for_request VARCHAR,
            session_id_for_start VARCHAR,
            session_id_for_finish VARCHAR,
            browser_id_for_request VARCHAR,
            browser_id_for_start VARCHAR,
            browser_id_for_finish VARCHAR,
            invalidation_description VARCHAR,
            pcnt_to_pass NUMERIC(3,2),
            passed BOOLEAN
        );
        """.format(output_schema=PUBLIC_SCHEMA),
        """
        INSERT INTO {output_schema}.assessment_takes (
            SELECT
                ats."id",
                ats."student_id",
                ats."num_correct",
                ats."created_at",
                ats."updated_at",
                ats."know_do_id",
                ats."num_possible",
                ats."taken_at",
                ats."approved_by_id",
                ats."assessment_id",
                ats."approved_time",
                ats."start_time",
                ats."is_content_assessment",
                ats."created_by",
                ats."updated_by",
                ats."ell_locale",
                ats."objective_level_stats",
                ats."additional_time_given",
                ats."academic_mindset_popup",
                ats."student_signature",
                ats."time_zone",
                ats."ip_for_request",
                ats."ip_for_start",
                ats."ip_for_finish",
                ats."session_id_for_request",
                ats."session_id_for_start",
                ats."session_id_for_finish",
                ats."browser_id_for_request",
                ats."browser_id_for_start",
                ats."browser_id_for_finish",
                ats."invalidation_description",
                kd."pcnt_to_pass",
                (100 * ats."num_correct" / cast(ats."num_possible" as float)) >= cast(round(kd."pcnt_to_pass" * 100, 0) as int)
            FROM {input_schema}.assessment_takes_{today} ats
            LEFT JOIN {input_schema}.know_dos_{today} kd
            ON (ats."know_do_id" = kd."id")
        );
        """.format(
            output_schema=PUBLIC_SCHEMA,
            input_schema=STAGING_SCRAPES_SCHEMA,
            today=DS_FOR_TABLE,
        ),
        'COMMIT;',
    ],
)
insert_assessment_takes.set_upstream([wait_for_assessment_takes, wait_for_know_dos])
