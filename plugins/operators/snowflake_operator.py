from snowflake.constants import SNOWFLAKE_CONN_ID, S3_BUCKET
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.jdbc_operator import JdbcOperator
from fb_required_args import require_keyword_args

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

class FBSnowflakeOperator(JdbcOperator):
    @require_keyword_args(['task_id', 'sql', 'dag'])
    def __init__(
        self,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        *args,
        **kwargs
    ):
        super(FBSnowflakeOperator, self).__init__(
            jdbc_conn_id=snowflake_conn_id,
            *args, **kwargs
        )

class FBSnowflakeCreateStageOperator(FBSnowflakeOperator):
    @require_keyword_args(['task_id', 'stage', 'file_format_name', 'file_format_sql', 'dag'])
    def __init__(
        self,
        s3_conn_id='s3_default',
        *args,
        **kwargs
    ):
        self.s3_conn_id = s3_conn_id
        self.stage = kwargs['stage']
        self.file_format_name = kwargs['file_format_name']
        self.file_format_sql = kwargs['file_format_sql']

        del kwargs['stage']
        del kwargs['file_format_name']
        del kwargs['file_format_sql']

        super(FBSnowflakeCreateStageOperator, self).__init__(
            sql=self._generate_transaction(),
            *args, **kwargs
        )

    def _generate_transaction(self):
        hook = FBS3Hook(s3_conn_id=self.s3_conn_id)
        a_key, s_key = hook.get_credentials()
        return [
            'BEGIN;',
            """
            CREATE OR REPLACE FILE FORMAT airflow.{name}
            {sql};
            """.format(name=self.file_format_name, sql=self.file_format_sql),
            """
            CREATE OR REPLACE STAGE airflow.{stage_name}
            FILE_FORMAT = airflow.{file_format_name}
            URL = 's3://{bucket}/'
            CREDENTIALS = (AWS_KEY_ID='{a_key}' AWS_SECRET_KEY='{s_key}');
            """.format(
                stage_name=self.stage,
                file_format_name=self.file_format_name,
                bucket=S3_BUCKET,
                a_key=a_key,
                s_key=s_key,
            ),
            'COMMIT;',
        ]
