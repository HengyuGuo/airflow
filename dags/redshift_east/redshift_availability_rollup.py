from airflow import DAG
from airflow.operators import (
    FBPostgresToS3Operator,
    FBS3ToRedshiftOperator,
)
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from redshift_east.constants import (
    REDSHIFT_CONN_ID,
    YESTERDAY_MACRO,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 13),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PARENT_DAG_NAME = 'redshift_availability_rollup'
SCHEDULE_INTERVAL = '@daily'
dag = DAG(
    PARENT_DAG_NAME,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
)

def sub_dag(child_dag, redshift_conn_id, airflow_task_id):
    dag = DAG(
        dag_id='{0}.{1}'.format(PARENT_DAG_NAME, child_dag),
        default_args=default_args,
        schedule_interval=SCHEDULE_INTERVAL,
    )

    sql = """
        SELECT JSON_BUILD_OBJECT(
            'ds',
            '{yesterday}',
            'id',
            job_id,
            'starttime',
            start_date,
            'endtime',
            end_date,
            'aborted',
            CASE state WHEN 'success' THEN 0 ELSE 1 END
        )
        FROM task_instance
        WHERE
            dag_id = 'redshift_availability_minutely' AND
            task_id = '{task_id}' AND
            start_date >= '{yesterday}' AND
            end_date < '{today}'
    """.format(
        yesterday=YESTERDAY_MACRO,
        task_id=airflow_task_id,
        today='{{ ds }}',
    )

    s3_key = '//plp-data-lake/import-staging/availability-check/{0}/{1}.json.gz'.format(
        child_dag,
        YESTERDAY_MACRO,
    )

    scrape = FBPostgresToS3Operator(
        task_id='scrape',
        sql=sql,
        postgres_conn_id='airflow_store',
        s3_key=s3_key,
        dag=dag,
    )

    load = FBS3ToRedshiftOperator(
        task_id='load',
        redshift_conn_id=redshift_conn_id,
        table='wild_west.availability_log',
        s3_key=s3_key,
        pre_sql="""
            -- TODO: Note that schema determination should replace this.
            -- But for now, we need a CREATE TABLE.
            CREATE TABLE IF NOT EXISTS wild_west.availability_log (
                ds date,
                id integer,
                starttime timestamp without time zone,
                endtime timestamp without time zone,
                aborted integer
            )
            SORTKEY (ds, id);

            DELETE FROM wild_west.availability_log
            WHERE ds = '{yesterday}';
        """.format(yesterday=YESTERDAY_MACRO),
        dag=dag,
    )
    load.set_upstream(scrape)
    return dag

west_coast = SubDagOperator(
    subdag=sub_dag('west_coast', 'redshift_west', 'west_coast_availability_check'),
    task_id='west_coast',
    dag=dag,
)

east_coast = SubDagOperator(
    subdag=sub_dag('east_coast', REDSHIFT_CONN_ID, 'east_coast_availability_check'),
    task_id='east_coast',
    dag=dag,
)
