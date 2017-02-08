from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
)
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)
from redshift import dim_helper

default_args = {
    'owner': 'hcli',
    'depends_on_past': False,
    'start_date': datetime(2016, 12, 12),
    'email': ['hcli@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PARENT_DAG_NAME = 'redshift_dim_know_dos'
SCHEDULE_INTERVAL = '@daily'

dag = DAG(PARENT_DAG_NAME, default_args=default_args, schedule_interval=SCHEDULE_INTERVAL)

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

dim_helper = SubDagOperator(
    task_id='dim_helper',
    subdag=dim_helper.sub_dag(
        parent_dag_name=PARENT_DAG_NAME,
        default_args=default_args,
        schedule_interval=SCHEDULE_INTERVAL,
        dim_table='dim_know_dos',
        input_schema=STAGING_SCRAPES_SCHEMA,
        output_schema=DIM_AND_FCT_SCHEMA,
        fields_sql="""
            id integer NOT NULL,
            name character varying(1020),
            subject_id integer,
            visibility character varying(256),
            subject_name character varying(256),
            core boolean,
            category character varying(256),
            as_of date NOT NULL
        """,
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
        sortkey='as_of, id',
    ),
    dag=dag,
)
dim_helper.set_upstream([wait_for_know_dos, wait_for_subjects])
