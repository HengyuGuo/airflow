from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    FBPostgresToS3CSVOperator,
    FBS3ToRedshiftOperator,
)
from airflow.hooks import FBCachedDbApiHook
from datetime import date, datetime, timedelta
from redshift.constants import (
    HEROKU_PUBLIC_SCHEMA,
    REDSHIFT_CONN_ID,
    SLAVE_DB_CONN_ID,
)

default_args = {
    'owner': 'ilan',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    'email': ['igoodman@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PARENT_DAG_NAME = 'redshift_csv_scrapes'
SCHEDULE_INTERVAL = '@daily'

main_dag = DAG(
    PARENT_DAG_NAME,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
)

def get_table_names():
    hook = FBCachedDbApiHook(conn_id=SLAVE_DB_CONN_ID)
    key = PARENT_DAG_NAME + '_get_table_names_' + str(date.today())
    records = hook.get_records(
        key=key,
        sql="""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
              AND table_name NOT IN (
                'google_tokens',
                'schema_migrations',
                'data_imports',
                'versions'
              )
            ORDER BY table_name
        """,
    )
    return [x[0] for x in records]

def get_scrape_subdag(table_name):
    dag = DAG(
        '{}.{}'.format(PARENT_DAG_NAME, table_name),
        default_args=default_args,
        schedule_interval=SCHEDULE_INTERVAL,
    )

    s3_key = '//plp-data-lake/import-staging/csv-scrapes/{}/{}.tsv.gz'.format(
        table_name,
        '{{ ds }}',
    )

    copy_to_s3_transaction = FBPostgresToS3CSVOperator(
        task_id='copy_to_s3_transaction',
        postgres_conn_id=SLAVE_DB_CONN_ID,
        table_name=table_name,
        s3_key=s3_key,
        dag=dag
    )

    # # TODO: work for any table (not just assessment_takes) by using the proper schema
    # upload_table = FBS3ToRedshiftOperator(
    #     task_id='upload_table',
    #     redshift_conn_id=REDSHIFT_CONN_ID,
    #     table='{schema}.{table_name}'.format(
    #         schema=HEROKU_PUBLIC_SCHEMA,
    #         table_name=table_name,
    #     ),
    #     s3_key=s3_key,
    #     pre_sql="""
    #         DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;
    #
    #         CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
    #             id integer NOT NULL,
    #             student_id integer NOT NULL,
    #             num_correct integer,
    #             created_at timestamp without time zone NOT NULL,
    #             updated_at timestamp without time zone NOT NULL,
    #             know_do_id integer NOT NULL,
    #             num_possible integer,
    #             taken_at timestamp without time zone,
    #             visibility integer,
    #             approved_by_id integer,
    #             assessment_id integer,
    #             approved_time timestamp without time zone,
    #             start_time timestamp without time zone,
    #             is_content_assessment boolean,
    #             invalidation_code integer,
    #             created_by integer,
    #             updated_by integer,
    #             ell_locale character varying(65535),
    #             objective_level_stats character varying(65535),
    #             additional_time_given character varying(65535),
    #             academic_mindset_popup boolean,
    #             student_signature character varying(65535),
    #             time_zone character varying(65535),
    #             ip_for_request character varying(65535),
    #             ip_for_start character varying(65535),
    #             ip_for_finish character varying(65535),
    #             session_id_for_request character varying(65535),
    #             session_id_for_start character varying(65535),
    #             session_id_for_finish character varying(65535),
    #             browser_id_for_request character varying(65535),
    #             browser_id_for_start character varying(65535),
    #             browser_id_for_finish character varying(65535),
    #             invalidation_description character varying(65535),
    #             started_by_teacher_id character varying(65535),
    #             finished_by_teacher_id character varying(65535)
    #         ) DISTSTYLE EVEN;
    #     """.format(
    #         schema=HEROKU_PUBLIC_SCHEMA,
    #         table_name=table_name,
    #     ),
    #     is_json=False,
    #     dag=dag,
    # )
    # upload_table.set_upstream(copy_to_s3_transaction)

    return dag

table_names = get_table_names()
for table_name in table_names:
    SubDagOperator(
        subdag=get_scrape_subdag(table_name),
        task_id=table_name,
        dag=main_dag
    )
