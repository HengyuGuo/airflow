import json
import logging

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from redshift.constants import REDSHIFT_DATA_TYPES

class FBS3Hook(S3Hook):
    def get_credentials(self):
        if self._creds_in_config_file:
            a_key, s_key, calling_format = _parse_s3_config(self.s3_config_file,
                                                            self.s3_config_format,
                                                            self.profile)
        elif self._creds_in_conn:
            a_key = self._a_key
            s_key = self._s_key
        return a_key, s_key

class FBS3ToRedshiftOperator(BaseOperator):
    template_fields = (
      'table',
      's3_key',
      'pre_sql',
      'schema_s3_key',
    )

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id,
            table,
            s3_key,
            pre_sql='',
            s3_conn_id='s3_default',
            s3_region='us-east-1',
            is_json=True,
            drop_and_create=False,
            schema_s3_key=None,
            *args, **kwargs):
        super(FBS3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_key = s3_key
        self.pre_sql = pre_sql
        self.s3_conn_id = s3_conn_id
        self.s3_region = s3_region
        self.is_json = is_json
        self.drop_and_create = drop_and_create
        self.schema_s3_key = schema_s3_key

    def _build_pre_sql(self):
        # A helper function that only needs to be called in the `_build_pre_sql` function
        def determine_schema():
            schema_sql = ''
            logging.info('Reading from s3: ' + self.schema_s3_key)
            schema_key = self.s3.get_key(self.schema_s3_key)
            # Schema must be stored as a JSONified array
            schema_array = json.loads(schema_key.get_contents_as_string())
            schema_strings = []

            for column in schema_array:
                column[0] = '"{}"'.format(column[0])
                # Replace any non-supported data types with 'text'
                if column[1].lower().split('(')[0] not in REDSHIFT_DATA_TYPES:
                    column[1] = 'text'

                schema_strings.append(' '.join(column))

            # Extra spaces added to make it look good in the logs
            return ',\n                '.join(schema_strings)

        pre_sql = """
            DROP TABLE IF EXISTS {table};

            CREATE TABLE IF NOT EXISTS {table} (
                {schema}
            ) DISTSTYLE EVEN;
        """.format(
            table=self.table,
            schema=determine_schema(),
        )

        return pre_sql

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = FBS3Hook(s3_conn_id=self.s3_conn_id)
        a_key, s_key = self.s3.get_credentials()

        json_or_tsv = ''
        if self.is_json:
            json_or_tsv = """
                JSON 'auto'
            """
        else:
            json_or_tsv = """
                DELIMITER AS '\t'
                ROUNDEC TRIMBLANKS ACCEPTANYDATE COMPUPDATE STATUPDATE
                FILLRECORD TRUNCATECOLUMNS NULL AS '\\\\N'
            """

        if self.drop_and_create:
            self.pre_sql += self._build_pre_sql()

        sql = """
            BEGIN;

            {pre_sql};

            COPY {table}
            FROM '{s3_path}'
            REGION '{s3_region}'
            CREDENTIALS 'aws_access_key_id={a_key};aws_secret_access_key={s_key}'
            {json_or_tsv}
            GZIP
            DATEFORMAT 'auto' TIMEFORMAT 'auto'
            MAXERROR 0;

            COMMIT;
        """.format(
            pre_sql=self.pre_sql,
            table=self.table,
            s3_path='s3:' + self.s3_key,
            s3_region=self.s3_region,
            a_key=a_key,
            s_key=s_key,
            json_or_tsv=json_or_tsv,
        )
        logging.info('Executing: ' + sql)
        self.hook.run(sql)
