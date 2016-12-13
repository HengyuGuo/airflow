from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class FBWriteSignalOperator(BaseOperator):
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
        *args,
        **kwargs
     ):
        super(FBWriteSignalOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.schema = schema
        self.table = table
        self.partition_id = partition_id

    def execute(self, context):
        hook = BaseHook.get_connection(self.conn_id).get_hook()
        # We don't create the table here. We could but if the signal
        # table is missing there's probably something more disastrously
        # wrong that should be looked into.
        #
        # For the record, it was created:
        # CREATE SCHEMA airflow;
        # CREATE TABLE airflow."signal" (schema_name TEXT, table_name TEXT, partition_id TEXT, status VARCHAR(256));
        sql = """
            INSERT INTO airflow."signal"
            (schema_name, table_name, partition_id, status)
            VALUES 
            ('{0}', '{1}', '{2}', '{3}')
        """.format(self.schema, self.table, self.partition_id, 'done')
        hook.run(sql)
