import logging

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

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
            *args, **kwargs):
        super(FBS3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_key = s3_key
        self.pre_sql = pre_sql
        self.s3_conn_id = s3_conn_id
        self.s3_region = s3_region
        self.is_json = is_json

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
