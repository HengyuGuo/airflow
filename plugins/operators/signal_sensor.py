import logging
from datetime import timedelta
from airflow.operators.sensors import BaseSensorOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

class FBSignalSensor(BaseSensorOperator):
    template_fields = (
      'schema',
      'table',
      'partition_id',
    )

    @apply_defaults
    def __init__(
        self,
        conn_id,
        schema,
        table,
        partition_id='as_of={{ ds }}',
        retry_delay=timedelta(seconds=600),
        retries=144,  # 600 seconds * 144 = 1 day
        *args,
        **kwargs
    ):
        super(FBSignalSensor, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.schema = schema
        self.table = table
        self.partition_id = partition_id
        self.email_on_retry = False
        self.retry_delay = retry_delay
        self.retries = retries

    def poke(self, context):
        hook = BaseHook.get_connection(self.conn_id).get_hook()
        sql = """
            SELECT COUNT(1)
            FROM airflow."signal"
            WHERE
                schema_name = '{0}' AND
                table_name = '{1}' AND
                partition_id = '{2}' AND
                status = 'done'
        """.format(self.schema, self.table, self.partition_id)
        logging.info('Poking: ' + sql)
        record = hook.get_first(sql)
        signal_present = record[0] > 0
        if not signal_present:
            raise AirflowException('Not present -- retry. If this is a test, then run the dependent job to fix the signal table issue.')
        return True
