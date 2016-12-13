import logging
from airflow.operators.sensors import BaseSensorOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

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
        poke_interval=5* 60,
        timeout=24 * 60 * 60,
        *args,
        **kwargs
    ):
        super(FBSignalSensor, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.schema = schema
        self.table = table
        self.partition_id = partition_id

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
        return signal_present
