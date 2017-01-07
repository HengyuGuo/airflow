from airflow import DAG
from airflow.operators import (
    FBRedshiftOperator,
    FBRedshiftToS3Transfer,
    FBSignalSensor,
    FBWriteSignalOperator,
)
from datetime import datetime, timedelta
from redshift_east.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)

default_args = {
    'owner': 'keoki',
    'depends_on_past': False,
    'start_date': datetime(2016, 12, 14),
    'email': ['kseu@summitps.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'register_udfs',
    default_args=default_args,
    schedule_interval='@daily',
)

udfs = {
    'popcount': {
        'desc': """
            Function returns number of binary 1s from an input datelist
            integer. n is the number of elements to look back in the datelist
            and shift is the offset to look back.
        """,
        'query': """
            CREATE OR REPLACE FUNCTION {function} (
                datelist int8, n int, shift int
            )
            returns int
            immutable
            as $$
                if shift > 0:
                    return bin(datelist)[-(n+shift):-shift].count('1')
                else:
                    return bin(datelist)[-n:].count('1')
            $$ language plpythonu;
        """,
    },
    'growthacc_status': {
        'desc': """
            Function returns the growthaccounting status for a user based on
            their activity. Read more about growthaccounting at
            fburl.com/growth_accounting.
        """,
        'query': """
            CREATE OR REPLACE FUNCTION {function} (
                datelist int8, day_active int, days_since_first_action int
            )
            returns varchar
            immutable
            as $$
                active_tdy = bin(datelist)[-day_active:].count('1') > 0
                active_ydy = bin(datelist)[-(day_active+1):-1].count('1') > 0

                new_user = days_since_first_action < day_active

                result = None
                if(active_ydy and active_tdy):
                    result = 'NEW' if new_user else 'RETAINED'
                elif (active_tdy and not active_ydy):
                    result = 'NEW' if new_user else 'RESURRECTED'
                elif (not active_tdy and not active_ydy):
                    result = 'STALE'
                elif (not active_tdy and active_ydy):
                    result = 'CHURN'

                return result
            $$ language plpythonu;
        """,
    },
}

queries = []
for function, config in udfs.items():
    desc = config.get('desc', '')
    query = config.get('query', None).format(function=function)
    if query is None:
        continue
    queries.append(query)

register_udfs = FBRedshiftOperator(
    task_id='register_udfs',
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="{{ params.queries }}",
    params={
      'queries': "\n".join(queries),
    },
    dag=dag,
)
