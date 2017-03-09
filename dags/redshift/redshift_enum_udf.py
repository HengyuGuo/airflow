from airflow import DAG
from airflow.operators import (
    FBRedshiftOperator,
    FBS3ToRedshiftOperator,
    FBRedshiftEnumUDFOperator,
)
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from constants import DEFAULT_SCHEDULE_INTERVAL
from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_WRITE_SCHEMA,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2016, 3, 5),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_enum_udf',
    default_args=default_args,
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
)

table_name = '{}.enums_translations'.format(STAGING_SCRAPES_WRITE_SCHEMA)

create_enums_translations = FBRedshiftOperator(
    task_id='create_enums_translations',
    sql="""
    BEGIN;
    CREATE TABLE IF NOT EXISTS {table_name} (
        table_name VARCHAR(256),
        class_name VARCHAR(256),
        "column" VARCHAR(256),
        key NUMERIC(10,0),
        value VARCHAR(256)
    )
    SORTKEY (table_name, class_name, "column", key);
    COMMIT;
    """.format(table_name=table_name),
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
)

s3_key = '//plp-data-lake/enums/translations_{{ ds }}.csv.gz'

load_enums_translations = FBS3ToRedshiftOperator(
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='load_enums_translations',
    table=table_name,
    s3_key=s3_key,
    is_json=False,
    pre_sql='DELETE FROM {};'.format(table_name),
    dag=dag,
)

load_enums_translations.set_upstream(create_enums_translations)

load_enum_udf = FBRedshiftEnumUDFOperator(
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='load_enum_udf',
    schema=STAGING_SCRAPES_WRITE_SCHEMA,
    table_name=table_name,
    dag=dag,
)

load_enum_udf.set_upstream(load_enums_translations)