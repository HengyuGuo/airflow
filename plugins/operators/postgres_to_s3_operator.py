import logging
import tempfile
import subprocess

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class FBPostgresHook(PostgresHook):
    def copy_expert(self, sql, file):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.copy_expert(sql, file)

class FBPostgresToS3Operator(BaseOperator):
    template_fields = (
      'sql',
      's3_key',
    )

    @apply_defaults
    def __init__(
            self,
            postgres_conn_id,
            sql,
            s3_key,
            s3_conn_id='s3_default',
            replace=True,
            *args, **kwargs):
        super(FBPostgresToS3Operator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id
        self.replace = replace

    def execute(self, context):
        self.hook = FBPostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.s3 = S3Hook(s3_conn_id=self.s3_conn_id)

        with tempfile.NamedTemporaryFile('w+b') as tmp_file:
            final_sql = "COPY (\n{}\n) TO STDOUT;".format(self.sql)
            logging.info('Writing to {0}: {1}'.format(tmp_file.name, final_sql))
            proc = subprocess.Popen(
                'sed \'s/\\\\"/\\"/g\' | gzip',
                stdin=subprocess.PIPE,
                stdout=tmp_file,
                shell=True,
            )
            self.hook.copy_expert(final_sql, proc.stdin)
            proc.communicate()

            logging.info('Writing to s3: ' + self.s3_key)
            self.s3.load_file(filename=tmp_file.name, key=self.s3_key, replace=self.replace)
