import json
import logging

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from snowflake.constants import (
    SNOWFLAKE_CONN_ID,
    POSTGRES_TO_SNOWFLAKE_DATA_TYPES,
    PASSTHROUGH_TYPE,
    STRING_TYPE,
    S3_BUCKET,
    DATE_TYPE,
)

from fb_required_args import require_keyword_args

class FBS3ToSnowflakeOperator(BaseOperator):
    template_fields = (
      'table',
      'data_s3_key',
      'pre_sql',
      'schema_s3_key',
    )

    @apply_defaults
    @require_keyword_args(['task_id', 'table', 'data_s3_key', 'stage', 'dag'])
    def __init__(
            self,
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            pre_sql=[],
            s3_conn_id='s3_default',
            drop_and_create=False,
            schema_s3_key=None,
            forced_string_columns=[],
            *args, **kwargs):
        self.snowflake_conn_id = snowflake_conn_id
        self.table = kwargs['table']
        self.data_s3_key = kwargs['data_s3_key']
        if isinstance(pre_sql, str):
            pre_sql = [pre_sql]
        elif not isinstance(pre_sql, list):
            raise TypeError('pre_sql must be str or list!')
        self.pre_sql = pre_sql
        self.s3_conn_id = s3_conn_id
        self.stage = kwargs['stage']
        self.drop_and_create = drop_and_create
        self.schema_s3_key = schema_s3_key
        self.forced_string_columns = forced_string_columns

        del kwargs['table']
        del kwargs['data_s3_key']
        del kwargs['stage']
        super(FBS3ToSnowflakeOperator, self).__init__(*args, **kwargs)

    def _build_pre_sql(self):
        # A helper function that only needs to be called in the `_build_pre_sql` function
        def determine_schema():
            schema_sql = ''
            logging.info('Reading from s3: ' + self.schema_s3_key)
            schema_key = self.s3.get_key(self.schema_s3_key)
            if schema_key is None:
                raise AirflowException('s3 key {} was not found. Did you forget to run a dependency?'.format(schema_key))
            # Schema must be stored as a JSONified array
            schema_array = json.loads(schema_key.get_contents_as_string())
            schema_strings = []

            for column in schema_array:
                column_name = column[0]
                column[0] = '"{}"'.format(column_name)

                # We're assuming well-formed type information
                type_and_len = column[1].lower().split('(')
                use_precise_type = (
                    type_and_len[0] in POSTGRES_TO_SNOWFLAKE_DATA_TYPES and
                    column_name not in self.forced_string_columns
                )
                if use_precise_type:
                    new_type = POSTGRES_TO_SNOWFLAKE_DATA_TYPES[type_and_len[0]]
                    if new_type != PASSTHROUGH_TYPE:
                        column[1] = new_type
                else:
                    # Replace any non-supported data types with the string type, aka VARCHAR
                    column[1] = STRING_TYPE

                schema_strings.append(' '.join(column))

            # Extra spaces added to make it look good in the logs
            return ',\n               '.join(schema_strings)

        pre_sql = [
           'DROP TABLE IF EXISTS {table};'.format(table=self.table),
           """
           CREATE TABLE IF NOT EXISTS {table} (
               {schema}
           );
           """.format(table=self.table, schema=determine_schema())
        ]
        return pre_sql

    def execute(self, context):
        self.hook = JdbcHook(jdbc_conn_id=self.snowflake_conn_id)
        self.s3 = S3Hook(s3_conn_id=self.s3_conn_id)

        sql = self.pre_sql
        if self.drop_and_create:
            sql += self._build_pre_sql()

        s3_bucket, s3_key = self.s3.parse_s3_url(self.data_s3_key)
        if s3_bucket != S3_BUCKET:
            raise ValueError(
              'For Snowflake loads the S3 bucket must be {}. Got: {}'.format(S3_BUCKET, s3_bucket)
            )
        copy_sql = """
            COPY INTO {table}
            FROM @airflow.{stage}/{s3_key};
        """.format(
            table=self.table,
            stage=self.stage,
            s3_key=s3_key,
        )
        sql.append(copy_sql)
        self.hook.run(['BEGIN;'] + sql + ['COMMIT;'])
