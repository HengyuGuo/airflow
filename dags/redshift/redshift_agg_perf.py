from airflow import DAG
from airflow.operators import (
    FBSignalSensor,
    FBRedshiftOperator,
    FBHistoricalOperator,
)
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from constants import DEFAULT_SCHEDULE_INTERVAL
from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)

default_args = {
    'owner': 'astewart',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 16),
    'email': ['astewart@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_agg_perf',
    default_args=default_args,
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
)

wait_for_sites = FBSignalSensor(
    task_id='wait_for_sites',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='sites',
    dag=dag,
)

wait_for_districts = FBSignalSensor(
    task_id='wait_for_districts',
    conn_id=REDSHIFT_CONN_ID,
    schema=STAGING_SCRAPES_SCHEMA,
    table='districts',
    dag=dag,
)

create_agg_perf = FBRedshiftOperator(
    task_id='create_agg_perf',
    sql="""
    BEGIN;
    CREATE TABLE IF NOT EXISTS {schema}.agg_perf_historical (
        as_of date NOT NULL,
        controller varchar(256),
        action varchar(256),
        release_version varchar(256),
        release_created_at varchar(256),
        duration_p10 float8,
        duration_p25 float8,
        duration_p50 float8,
        duration_p75 float8,
        duration_p90 float8,
        duration_p95 float8,
        duration_cnt int8,
        db_p10 float8,
        db_p25 float8,
        db_p50 float8,
        db_p75 float8,
        db_p90 float8,
        db_p95 float8,
        db_cnt int8,
        response_size_p10 float8,
        response_size_p25 float8,
        response_size_p50 float8,
        response_size_p75 float8,
        response_size_p90 float8,
        response_size_p95 float8,
        response_size_cnt int8
    )
    SORTKEY (as_of, controller, action, release_version);
    COMMIT;
    """.format(schema=DIM_AND_FCT_SCHEMA),
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
)

insert_agg_perf = FBHistoricalOperator(
    redshift_conn_id=REDSHIFT_CONN_ID,
    task_id='insert_agg_perf',
    view_name='agg_perf',
    select_sql="""
    SELECT
        '{{ ds }}' AS date,
        ps.controller,
        ps.action,
        ps.release_version,
        ps.release_created_at,
        ps.duration_p10,
        ps.duration_p25,
        ps.duration_p50,
        ps.duration_p75,
        ps.duration_p90,
        ps.duration_p95,
        cnt.duration_cnt,
        ps.db_p10,
        ps.db_p25,
        ps.db_p50,
        ps.db_p75,
        ps.db_p90,
        ps.db_p95,
        cnt.db_cnt,
        ps.response_size_p10,
        ps.response_size_p25,
        ps.response_size_p50,
        ps.response_size_p75,
        ps.response_size_p90,
        ps.response_size_p95,
        cnt.response_size_cnt
    FROM (
    SELECT DISTINCT
        controller,
        action,
        release_version,
        release_created_at,
        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY duration)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS duration_p10,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY duration)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS duration_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS duration_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY duration)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS duration_p75,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY duration)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS duration_p90,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS duration_p95,
        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY db)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS db_p10,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY db)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS db_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY db)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS db_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY db)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS db_p75,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY db)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS db_p90,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY db)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS db_p95,
        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY response_size)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS response_size_p10,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY response_size)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS response_size_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_size)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS response_size_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY response_size)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS response_size_p75,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY response_size)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS response_size_p90,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_size)
            OVER(PARTITION BY controller, action,
            release_version, release_created_at)
        AS response_size_p95
    FROM http_logs_prod
    WHERE DATE_TRUNC('day', "timestamp") = '{{ ds }}'
    ) ps
    JOIN (
    SELECT
        controller,
        action,
        release_version,
        release_created_at,
        COUNT(duration) AS duration_cnt,
        COUNT(db) AS db_cnt,
        COUNT(response_size) AS response_size_cnt
    FROM http_logs_prod
    WHERE DATE_TRUNC('day', "timestamp") = '{{ ds }}'
    GROUP BY
        controller,
        action,
        release_version,
        release_created_at
    ) cnt
    ON COALESCE(ps.controller, '____null____') = COALESCE(cnt.controller, '____null____')
    AND COALESCE(ps.action, '____null____') = COALESCE(cnt.action, '____null____')
    AND COALESCE(ps.release_version, '____null____')
        = COALESCE(cnt.release_version, '____null____')
    AND COALESCE(ps.release_created_at, '____null____')
        = COALESCE(cnt.release_created_at, '____null____');
    """,
    dag=dag,
)

insert_agg_perf.set_upstream([
    wait_for_sites, 
    wait_for_districts,
    create_agg_perf,
])
