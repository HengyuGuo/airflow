from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    FBRedshiftOperator,
    FBRedshiftToS3Transfer,
    FBWriteSignalOperator,
)
from airflow.hooks import FBCachedDbApiHook
from datetime import date, datetime, timedelta
from constants import DEFAULT_SCHEDULE_INTERVAL
from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_WRITE_SCHEMA,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2016, 12, 8),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PARENT_DAG_NAME = 'redshift_nightly_plp_scrapes'
SCHEDULE_INTERVAL = DEFAULT_SCHEDULE_INTERVAL

main_dag = DAG(
    PARENT_DAG_NAME,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
)

def get_table_names():
    hook = FBCachedDbApiHook(conn_id=REDSHIFT_CONN_ID)
    key = PARENT_DAG_NAME + '_get_table_names_' + str(date.today())
    records = hook.get_records(
        key=key,
        sql="""
            SELECT table_name::text
            FROM information_schema.tables
            WHERE table_schema = 'heroku_public'
            AND table_name NOT IN ('fivetran_audit')
            ORDER BY 1
        """,
    )
    return [x[0] for x in records]

def get_scrape_subdag(table_name):
    dag = DAG(
        '%s.%s' % (PARENT_DAG_NAME, table_name),
        default_args=default_args,        
        schedule_interval=SCHEDULE_INTERVAL,
    )
    copy_transaction = FBRedshiftOperator(
        task_id='copy_transaction',
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql="""
            BEGIN;

            DROP TABLE IF EXISTS
            {{ params.schema }}."{{ params.table_name }}_{{ macros.ds_add(ds, -4) }}"
            CASCADE; -- CASCADE will DROP the VIEW, too
            DROP TABLE IF EXISTS {{ params.schema }}."{{ params.table_name }}_{{ ds }}" CASCADE;
            DROP VIEW IF EXISTS {{ params.schema }}."{{ params.table_name }}" CASCADE;            

            -- Copy Table
            -- Step 0: Drop
            DROP TABLE IF EXISTS {{ params.schema }}."{{ params.table_name }}";
            -- Step 1: Create
            CREATE TABLE {{ params.schema }}."{{ params.table_name }}"
            (LIKE heroku_public."{{ params.table_name }}");
            -- Step 2: Copy
            INSERT INTO {{ params.schema }}."{{ params.table_name }}"
            SELECT * FROM heroku_public."{{ params.table_name }}";

            GRANT ALL PRIVILEGES ON {{ params.schema }}."{{ params.table_name }}"
            TO GROUP data_eng;

            ALTER TABLE {{ params.schema }}.{{ params.table_name }}
            RENAME TO "{{ params.table_name }}_{{ ds }}";

            CREATE VIEW {{ params.schema }}.{{ params.table_name }} AS
            (SELECT * FROM {{ params.schema }}."{{ params.table_name }}_{{ ds }}");

            GRANT ALL PRIVILEGES ON {{ params.schema }}.{{ params.table_name }}
            TO GROUP data_eng;

            COMMIT;
        """,
        params={
          'schema': STAGING_SCRAPES_WRITE_SCHEMA,
          'table_name': table_name,
        },
        dag=dag,
    )

    signal = FBWriteSignalOperator(
        conn_id=REDSHIFT_CONN_ID,
        task_id='write_signal',
        schema=STAGING_SCRAPES_WRITE_SCHEMA,
        table=table_name,
        dag=dag,
    )
    signal.set_upstream(copy_transaction)

    unload = FBRedshiftToS3Transfer(
        task_id='unload',
        schema=STAGING_SCRAPES_WRITE_SCHEMA,
        table='{{ params.table_name }}_{{ ds }}',
        s3_bucket='plp-data-lake',
        s3_key='scrapes-opt-prod-airflow/{{ params.table_name }}/as_of={{ ds }}/',
        redshift_conn_id=REDSHIFT_CONN_ID,
        unload_options=[
            'ALLOWOVERWRITE',
            'DELIMITER AS \',\'',
            'GZIP',
            'ESCAPE ADDQUOTES',
        ],
        params={'table_name': table_name},
        dag=dag,
    )
    unload.set_upstream(copy_transaction)

    return dag

table_names = get_table_names()
for table_name in table_names:
    SubDagOperator(
        subdag=get_scrape_subdag(table_name),
        task_id=table_name,
        dag=main_dag
    )
