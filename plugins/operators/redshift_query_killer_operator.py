import datetime
import boto
import json
import logging
import re
import subprocess

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from operators.constants import MESSAGE_SNS_TOPIC

def json_datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")

# Fake data with slow @kristen queries. This makes testing changes easier
# since there are actually some slow queries.
CURRENT_STATE_SQL_TEST = 'SELECT *, -1 AS pid FROM wild_west."warehouse_status_2017-01-11";'
# The real query which queries wlm tables for running queries.
CURRENT_STATE_SQL_DEFAULT = """
set query_group to superuser;
-- Test Query
SELECT * FROM (
    SELECT
        swqs.query as query_id,
        u.usename AS user,
        swqs.wlm_start_time AS start_time,
        swqs.exec_time / 1000000 / 60 AS minutes_running,
        swqs.queue_time / 1000000 / 60 AS minutes_queued,
        REPLACE(LISTAGG(sq.text, '') WITHIN GROUP (ORDER BY sq.sequence), '\\n', '\n') AS sql,
        sq.pid AS pid
    FROM stv_wlm_query_state swqs
    JOIN stl_querytext sq
        ON swqs.query = sq.query
        AND sq.sequence < 327  -- 200 * 327 = 65400 characters which clears the 65535 limit for LISTAGG
    JOIN pg_user u ON sq.userid = u.usesysid
    GROUP BY 1, 2, 3, 4, 5, 7
) WHERE sql NOT LIKE '-- Test Query%';
"""

class FBRedshiftQueryKillerOperator(BaseOperator):
    template_fields = (
        'log_s3_key',
        'dry_run',
        'current_state_sql',
        'log_table',
    )

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id,
            log_s3_key,
            query_ttl_minutes = 60,
            dry_run='{% if test_mode %}1{% endif %}',
            current_state_sql='{% if test_mode %}test{% else %}default{% endif %}',
            log_table='{% if test_mode %}wild_west.query_killer_log{% endif %}',
            s3_conn_id = 's3_default',
            *args, **kwargs):
        super(FBRedshiftQueryKillerOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.log_s3_key = log_s3_key
        self.query_ttl_minutes = query_ttl_minutes
        self.dry_run = dry_run
        self.current_state_sql = current_state_sql
        self.log_table = log_table
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        now = datetime.datetime.now()
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(s3_conn_id=self.s3_conn_id)
        if self.current_state_sql == 'test':
            sql = CURRENT_STATE_SQL_TEST
        elif self.current_state_sql == 'default':
            sql = CURRENT_STATE_SQL_DEFAULT
        else:
            raise Exception('current_state_sql should be one of "test" or "default"')
        if self.dry_run:
            logging.info('This is a dry-run. It won\'t kill queries or announce in slack.')
            logging.info('By default, we also use fake data. That makes it easier to test when there are no slow queries running.')
        logging.info('Getting current state: ' + sql)
        current_state = self.hook.get_records(sql)
        dicts = []
        for row in current_state:
            d = dict(zip(['query_id', 'user', 'start_time', 'minutes_running', 'minutes_queued', 'sql', 'pid'], row))
            is_load = re.match('^\w*copy', d['sql'], flags=re.IGNORECASE)
            if is_load:
                logging.info('Exclude query {}: is load', d['query_id'])
            # This iteration of the query killer just looks at the length the query has
            # been running. We could get more fancy and kill queries sooner when the queue
            # is full, but we don't do that now (and might not ever).
            too_long = (not is_load) and d['minutes_running'] >= self.query_ttl_minutes
            if too_long:
                logging.info('Query running too long, id:{0} pid:{1} sql:{2}'.format(d['query_id'], d['pid'], d['sql']))
            d['killer_run_time'] = now
            d['too_long'] = too_long
            dicts.append(d)

        # Dump data for logging. This can help us debug which queries are running
        # longer and give proof when people complain why the query was killed.
        jsons = "\n".join([json.dumps(d, default=json_datetime_handler) for d in dicts])
        proc = subprocess.Popen(
            'gzip',
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            shell=True,
        )
        stdout, stderr = proc.communicate(jsons)
        logging.info('Dumping to S3 key: ' + self.log_s3_key)
        self.s3.load_string(string_data=stdout, key=self.log_s3_key, replace=True)

        if not self.dry_run:
            sns = None
            kill_sql = ''
            for d in dicts:
                if d['too_long']:
                    if d['pid'] < 0:
                        raise Exception('Test data unexpectedly was run with !dry_run.')
                    kill_sql += "\nCANCEL {} \'Long-running query automatically killed, contact @astewart for more info\';".format(d['pid'])
                    if sns is None:
                        sns = boto.sns.connect_to_region('us-east-1')
                    message = "Query killer killing query_id {0} by user '{1}' for running more than {2} minutes:\n{3}".format(
                        d['query_id'],
                        d['user'],
                        self.query_ttl_minutes,
                        d['sql'],
                    )
                    sns.publish(topic=MESSAGE_SNS_TOPIC, message=message)

            if kill_sql:
                self.hook.run(kill_sql, autocommit=True)
