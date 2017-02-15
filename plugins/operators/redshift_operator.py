from redshift.constants import (
    REDSHIFT_CONN_ID,
    STAGING_SCRAPES_SCHEMA,
    DIM_AND_FCT_SCHEMA,
)

from airflow.operators.postgres_operator import PostgresOperator
from fb_required_args import require_keyword_args

class FBRedshiftOperator(PostgresOperator):
    pass

class FBHistoricalOperator(FBRedshiftOperator):

    @require_keyword_args(['task_id', 'view_name', 'select_sql', 'dag'])
    def __init__(
        self,
        redshift_conn_id=REDSHIFT_CONN_ID,
        input_schema=STAGING_SCRAPES_SCHEMA,
        output_schema=DIM_AND_FCT_SCHEMA,
        *args,
        **kwargs
    ):
        self.input_schema = input_schema
        self.output_schema = output_schema
        self.view_name = kwargs['view_name']
        self.select_sql = kwargs['select_sql']
        del kwargs['view_name']
        del kwargs['select_sql']

        super(FBHistoricalOperator, self).__init__(
            postgres_conn_id=redshift_conn_id,
            sql=self._generate_transaction(),
            params=self._generate_params(),
            *args, **kwargs
        )

    def _generate_transaction(self):
        delete_stmt = """
            DELETE FROM {{ params.output_schema }}."{{ params.historical_table }}" WHERE as_of = '{{ ds }}';
        """
        insert_stmt = """
            INSERT INTO {{ params.output_schema }}."{{ params.historical_table }}"
        """ + self.select_sql + ';'
        insert_signal = """
            {% if not test_mode %}
                INSERT INTO airflow."signal"
                (schema_name, table_name, partition_id, status)
                VALUES
                ('{{ params.output_schema }}', '{{ params.historical_table}}', 'as_of={{ ds }}', 'done');
            {% endif %}
        """
        create_view = """
            DROP VIEW IF EXISTS {{ params.output_schema }}."{{ params.view_name }}";
            CREATE VIEW {{ params.output_schema }}."{{ params.view_name }}" AS
                SELECT * FROM {{ params.output_schema }}."{{ params.historical_table }}" 
                WHERE as_of =  (
                    SELECT MAX(as_of)
                    FROM {{ params.output_schema }}."{{ params.historical_table }}" 
                );
        """
        view_signal = """
            {% if not test_mode %}
                INSERT INTO airflow."signal"
                (schema_name, table_name, partition_id, status)
                VALUES
                ('{{ params.output_schema }}', '{{ params.view_name}}', 'as_of={{ ds }}', 'done');
            {% endif %}
        """

        return '\n'.join([
            'BEGIN;',
            delete_stmt,
            insert_stmt,
            insert_signal,
            create_view,
            view_signal,
            'COMMIT;'
        ])

    def _generate_params(self):
        return {
            'view_name': self.view_name,
            'historical_table': self.view_name + '_historical',
            'output_schema': self.output_schema,
            'input_schema': self.input_schema,
        }
