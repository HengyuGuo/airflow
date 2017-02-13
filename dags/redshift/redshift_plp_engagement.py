from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    FBRedshiftOperator,
    FBRedshiftToS3Transfer,
    FBSignalSensor,
    FBWriteSignalOperator,
)
from redshift.constants import (
    REDSHIFT_CONN_ID,
    DIM_AND_FCT_SCHEMA,
)

from datetime import datetime, timedelta

default_args = {
    'owner': 'keoki',
    'depends_on_past': False,
    'start_date': datetime(2016, 10, 16),
    'email': ['kseu@summitps.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_plp_engagement',
    default_args=default_args,
    schedule_interval='@daily',
)

def def_delete(table):
    d = "DELETE FROM " + table + " WHERE as_of = '{{ ds }}';"
    return d

creates = {
    'daily': """
        CREATE TABLE IF NOT EXISTS {{ params.daily }} (
            as_of DATE NOT NULL,
            acting_user_id int4,
            curr_user_id int4,
            controller varchar(256),
            action varchar(256),
            status varchar(256),
            num_action int4
        );
    """,
    'datelist': """
        CREATE TABLE IF NOT EXISTS {{ params.datelist }} (
            as_of DATE NOT NULL,
            acting_user_id int4,
            curr_user_id int4,
            controller varchar(256),
            action varchar(256),
            status varchar(256),
            todays_num_action int4,
            datelist_int int4,
            first_action_date date,
            last_action_date date
        );
    """,
    'visitation': """
        CREATE TABLE IF NOT EXISTS {{ params.visitation }} (
            as_of DATE NOT NULL,
            aggregation varchar(64),
            curr_user_id int4,
            controller varchar(256),
            action varchar(256),
            status varchar(256),
            todays_num_action int4,
            datelist_int int4,
            first_action_date date,
            last_action_date date,
            l1 int2,
            l7 int2,
            l28 int2,
            l1_ga_status varchar(16),
            l7_ga_status varchar(16),
            l28_ga_status varchar(16)
        );
    """,
}

aggregations = [
    ['overall'],
    ['action'],
    ['controller'],
    ['controller', 'action'],
    ['controller', 'action', 'status'],
]

activity_cols = ['controller', 'action', 'status']

params = {
    'daily': 'fct_engagement_daily',
    'datelist': 'fct_engagement_datelist',
    'visitation': 'fct_engagement_visitation',
    'datelist_max': 2**30,
}

create = FBRedshiftOperator(
    dag=dag,
    params=params,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="\n".join(creates.values()),
    task_id='creates',
)
daily = FBRedshiftOperator(
    dag=dag,
    params=params,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=def_delete("{{ params.daily }}") + """
        INSERT INTO {{ params.daily }}
        SELECT
            timestamp::date AS as_of,
            acting_user_id,
            curr_user_id,
            controller,
            action,
            status,
            COUNT(*) AS num_action
        FROM http_logs_prod
        WHERE
            timestamp::date = '{{ ds }}'
            AND (acting_user_id IS NOT NULL OR curr_user_id IS NOT NULL)
        GROUP BY
            1, 2, 3, 4, 5, 6
    """,
    task_id='daily',
)
daily.set_upstream(create)

datelist = FBRedshiftOperator(
    dag=dag,
    params=params,
    postgres_conn_id=REDSHIFT_CONN_ID,
    depends_on_past=True,
    sql=def_delete("{{ params.datelist }}") + """
        INSERT INTO {{ params.datelist }}
        SELECT
            CAST('{{ds}}' AS DATE) as_of,
            COALESCE(d.acting_user_id, dl.acting_user_id) AS acting_user_id,
            COALESCE(d.curr_user_id, dl.curr_user_id) AS curr_user_id,
            COALESCE(d.controller, dl.controller) AS controller,
            COALESCE(d.action, dl.action) AS action,
            COALESCE(d.status, dl.status) AS status,
            d.num_action AS todays_num_action,
            (CASE
                WHEN dl.datelist_int IS NULL THEN 0
                WHEN dl.datelist_int > {{ params.datelist_max }}
                THEN dl.datelist_int - {{ params.datelist_max }}
                ELSE dl.datelist_int
            END << 1) +
            CASE
                WHEN d.num_action > 0 THEN 1
                ELSE 0
            END AS datelist_int,
            COALESCE(dl.first_action_date, d.as_of) AS first_action_date,
            COALESCE(d.as_of, dl.last_action_date) AS last_action_date
        FROM {{ params.datelist }} dl
        FULL OUTER JOIN {{ params.daily }} d
        ON dl.acting_user_id = d.acting_user_id
        AND dl.curr_user_id = d.curr_user_id
        AND dl.controller = d.controller
        AND dl.action = d.action
        AND dl.status = d.status
        AND d.as_of = '{{ ds }}'
        WHERE dl.as_of = '{{ macros.ds_add(ds, -1) }}'
    """,
    task_id='datelist',
)
datelist.set_upstream(daily)

visitation_query = """
    INSERT INTO {{ params.visitation }}
    SELECT
        CAST('{{ ds }}' AS date) AS as_of,
        aggregation,
        curr_user_id,
        controller,
        action,
        status,
        todays_num_action,
        datelist_int,
        first_action_date,
        last_action_date,
        popcount(datelist_int, 1, 0) AS l1,
        popcount(datelist_int, 7, 0) AS l7,
        popcount(datelist_int, 28, 0) AS l28,
        growthacc_status(datelist_int, 1, days_since_first_action)
            AS l1status,
        growthacc_status(datelist_int, 7, days_since_first_action)
            AS l7status,
        growthacc_status(datelist_int, 28, days_since_first_action)
            AS l28status
    FROM (
        SELECT
            curr_user_id,
            '{{ params.aggregation }}' AS aggregation,
            {{ params.action_cols }},
            SUM(todays_num_action) AS todays_num_action,
            BIT_OR(datelist_int) AS datelist_int,
            MIN(first_action_date) AS first_action_date,
            MAX(last_action_date) AS last_action_date,
            CAST(DATEDIFF('day',
                MIN(first_action_date), CAST('{{ ds }}' AS date)
            ) AS INT) AS days_since_first_action
        FROM {{ params.datelist }}
        WHERE as_of = '{{ ds }}'
        {{ params.groupby }}
    ) a

"""
for agg in aggregations:
    aggregation = ",".join(agg)
    params['aggregation'] = aggregation

    cols = []
    for a in activity_cols:
        if a in agg:
            cols.append(a)
        else:
            cols.append("'overall' AS %s" % a)
    params['action_cols'] = ",\n".join(cols)

    groupby_vals = ['curr_user_id']
    if not (len(agg) == 1 and agg[0] == 'overall'):
        groupby_vals.extend(agg)
    params['groupby'] = 'GROUP BY ' + ','.join(groupby_vals)

    delete = """
        DELETE FROM {{ params.visitation }} WHERE as_of = '{{ ds }}'
            AND aggregation = '{{ params.aggregation }}';
    """

    visitation = FBRedshiftOperator(
        dag=dag,
        params=params,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=delete + visitation_query,
        task_id='visitation_' + '_'.join(agg),
    )
    visitation.set_upstream(datelist)