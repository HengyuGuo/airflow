from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import (
    FBSNSOperator,
    FBExternalDagRunSensor,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 21),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_fact_and_snapshot_message',
    default_args=default_args,
    schedule_interval='@daily',
)

CHECK_THESE_DAGS = [
    'redshift_assessment_takes',
    'redshift_course_assignment_sections',
    'redshift_course_assignments',
    'redshift_dim_courses',
    'redshift_dim_know_dos',
    'redshift_dim_parents',
    'redshift_dim_sites',
    'redshift_dim_students',
    'redshift_dim_teachers',
    'redshift_know_do_masteries',
    'redshift_project_assignment_dimension_scores',
    'redshift_sections',
    'redshift_section_teachers',
]

checks = [
    FBExternalDagRunSensor(task_id=dag_id, wait_for_dag_id=dag_id, dag=dag) for dag_id in CHECK_THESE_DAGS
]

sns_message = FBSNSOperator(
    task_id='sns_message',
    message="Success on all fact and snapshot tables for as_of={{ ds }}.\n\nChecked:\n" + "\n".join([
        '`{}`'.format(dag_id[9:]) for dag_id in CHECK_THESE_DAGS  # [9:] cuts off "redshift_"
    ]),
    dag=dag,
)
sns_message.set_upstream(checks)
