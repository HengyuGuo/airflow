import logging
from datetime import timedelta
from airflow.operators import S3KeySensor
from airflow.operators.sensors import BaseSensorOperator
from airflow.hooks import S3Hook
from airflow.exceptions import AirflowException

from fb_required_args import require_keyword_args

class FBS3KeySensor(BaseSensorOperator):
    template_fields = (
      's3_key',
    )

    @require_keyword_args(['task_id', 's3_key', 'dag'])
    def __init__(
            self,
            s3_conn_id='s3_default',
            retry_delay=timedelta(seconds=600),
            retries=144,  # 600 seconds * 144 = 1 day
            *args, **kwargs):
        self.s3_key = kwargs['s3_key']
        self.s3_conn_id = s3_conn_id
        self.email_on_failure = True
        self.email_on_retry = False
        self.retry_delay = retry_delay
        self.retries = retries

        del kwargs['s3_key']
        super(FBS3KeySensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        hook = S3Hook(s3_conn_id=self.s3_conn_id)
        bucket, key = s3.parse_s3_url(self.s3_key)
        full_url = 's3://' + bucket + '/' + key

        logging.info('Poking for key : {full_url}'.format(**locals()))
        if hook.check_for_key(key, bucket):
            return True

        raise AirflowException('Not present -- retry. If this is a test, then run the dependent job to fix the S3 hook issue.')
