from airflow import DAG
from airflow.operators import FBSignalSensor
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)
from redshift import dim_helper

default_args = {
    'owner': 'tpham',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 9),
    'email': ['tpham@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PARENT_DAG_NAME = 'redshift_project_assignment_dimension_scores_historical'
SCHEDULE_INTERVAL = '@daily'

dag = DAG(PARENT_DAG_NAME, default_args=default_args, schedule_interval=SCHEDULE_INTERVAL)

wait_for_project_assignments = FBSignalSensor(
    task_id='wait_for_project_assignments',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='project_assignments',
    dag=dag,
)

wait_for_project_assignment_dimension_scores = FBSignalSensor(
    task_id='wait_for_project_assignment_dimension_scores',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='project_assignment_dimension_scores',
    dag=dag,
)

dim_helper = SubDagOperator(
    task_id='dim_helper',
    subdag=dim_helper.sub_dag(
        parent_dag_name=PARENT_DAG_NAME,
        default_args=default_args,
        schedule_interval=SCHEDULE_INTERVAL,
        dim_table='project_assignment_dimension_scores_historical',
        input_schema=STAGING_SCRAPES_SCHEMA,
        output_schema=DIM_AND_FCT_SCHEMA,
        fields_sql="""
            as_of date NOT NULL,
            project_id numeric(10,0) NOT NULL,
            student_id numeric(10,0) NOT NULL,
            type character varying(65535),
            project_assignment_id numeric(10,0),
            concept_dimension_id numeric(10,0),
            cog_skill_dimension_id numeric(10,0),
            score numeric(4,2),
            scores_id numeric(10,0) NOT NULL,
            created_at timestamp without time zone NOT NULL,
            updated_at timestamp without time zone NOT NULL,
            created_by numeric(10,0),
            updated_by numeric(10,0),
            draft_score character varying(1024),
            self_assessed_score character varying(1024)
        """,
        select_sql="""
            SELECT
                TO_DATE('{{ ds }}', 'YYYY-MM-DD') as as_of,
                pa.project_id,
                pa.student_id,
                pads.type,
                pads.project_assignment_id,
                pads.concept_dimension_id,
                pads.cog_skill_dimension_id,
                CAST(score as numeric(4, 2)) as score,
                pads.id as scores_id,
                pads.created_at,
                pads.updated_at,
                pads.created_by,
                pads.updated_by,
                pads.draft_score,
                pads.self_assessed_score
            FROM {{ params.input_schema }}.project_assignments_{{ ds }} pa
            JOIN {{ params.input_schema }}.project_assignment_dimension_scores_{{ ds }} pads
            ON (pa.id = pads.project_assignment_id);
        """,
        distkey='student_id',
        sortkey='as_of, project_id, student_id, project_assignment_id',
    ),
    dag=dag,
)

dim_helper.set_upstream([
    wait_for_project_assignments, 
    wait_for_project_assignment_dimension_scores
])
