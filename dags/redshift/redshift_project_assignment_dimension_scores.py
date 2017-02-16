from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
    FBHistoricalOperator,
    FBRedshiftOperator,
)
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

dag = DAG('redshift_project_assignment_dimension_scores', 
    default_args=default_args, 
    schedule_interval='@daily')

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

create_project_assignment_dimension_scores = FBRedshiftOperator(
    task_id='create_project_assignment_dimension_scores',
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="""
    BEGIN;
    CREATE TABLE IF NOT EXISTS {schema}.project_assignment_dimension_scores_historical (
        as_of date NOT NULL,
        project_id numeric(10,0) NOT NULL,
        student_id numeric(10,0) NOT NULL,
        type character varying(65535),         
        project_assignment_id numeric(10,0),
        concept_dimension_id numeric(10,0),
        cog_skill_dimension_id numeric(10,0),
        score numeric(4,2),                                    
        scores_id numeric(10,0) NOT NULL,
        created_at timestamp without time zone,                                                         
        updated_at timestamp without time zone,
        created_by numeric(10,0),                    
        updated_by numeric(10,0),
        draft_score character varying(1024),
        self_assessed_score character varying(1024)
    )
    SORTKEY (as_of, project_id, student_id);
    COMMIT;
    """.format(schema=DIM_AND_FCT_SCHEMA),
    dag=dag,
)

insert_project_assignment_dimension_scores = FBHistoricalOperator(
    task_id='insert_project_assignment_dimension_scores',
    view_name='project_assignment_dimension_scores',
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
        FROM {{ params.input_schema }}."project_assignments_{{ ds }}" pa
        JOIN {{ params.input_schema }}."project_assignment_dimension_scores_{{ ds }}" pads
        ON (pa.id = pads.project_assignment_id);
    """,
    dag=dag,
)

insert_project_assignment_dimension_scores.set_upstream([
    wait_for_project_assignments, 
    wait_for_project_assignment_dimension_scores,
    create_project_assignment_dimension_scores,
])
