from airflow import DAG
from airflow.operators import (
    FBHistoricalOperator,
    FBSignalSensor,
)
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)

default_args = {
    'owner': 'hcli',
    'depends_on_past': False,
    'start_date': datetime(2016, 2, 15),
    'email': ['hcli@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('redshift_dim_know_dos', default_args=default_args, schedule_interval='@daily')

wait_for_know_dos = FBSignalSensor(
    task_id='wait_for_know_dos',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='know_dos',
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
    view_name='dim_know_dos',
    select_sql="""
        SELECT
            kd.id AS id,
            kd.name AS name,
            kd.subject_id AS subject_id,
            enum_name_for_value('visibility', kd.visibility, 'know_dos', 'KnowDo') AS visibility,
            s.name AS subject_name,
            CASE s.core WHEN 't' THEN TRUE WHEN 'f' THEN FALSE ELSE NULL END AS core,
            enum_name_for_value('category', s.category, 'Subjects', 'Subject') AS category,
            TO_DATE('{{ ds }}', 'YYYY-MM-DD') AS as_of
        FROM "{{ params.input_schema }}"."know_dos_{{ ds }}" kd
        JOIN "{{ params.input_schema }}"."subjects_{{ ds }}" s
        ON (kd.subject_id = s.id);
    """,
    dag=dag,
)
select_and_insert.set_upstream([wait_for_know_dos, wait_for_subjects])
