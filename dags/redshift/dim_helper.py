from airflow import DAG
from airflow.operators import FBRedshiftOperator, FBWriteSignalOperator
from redshift.constants import REDSHIFT_CONN_ID

def sub_dag(
    parent_dag_name,
    default_args,
    schedule_interval,
    dim_table,
    input_schema,
    output_schema,
    fields_sql,
    select_sql,
    sortkey='as_of, id',
    conn_id=REDSHIFT_CONN_ID,
):
    dag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, 'dim_helper'),
        default_args=default_args,
        schedule_interval=schedule_interval,
    )

    dim_transaction = FBRedshiftOperator(
        task_id='dim_transaction',
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql="""
            BEGIN;

            CREATE TABLE IF NOT EXISTS {{ params.output_schema }}."{{ params.dim_table }}_historical" (
                {{ params.fields_sql }}
            )
            SORTKEY ({{ params.sortkey }});

            DELETE FROM {{ params.output_schema }}."{{ params.dim_table }}_historical" WHERE as_of = '{{ ds }}';

            INSERT INTO {{ params.output_schema }}."{{ params.dim_table }}_historical" """
            + select_sql + # select_sql may itself be templated so paste it in
            """;

            DROP VIEW IF EXISTS {{ params.output_schema }}."{{ params.dim_table }}";
            CREATE VIEW {{ params.output_schema }}."{{ params.dim_table }}" AS
                SELECT * FROM {{ params.output_schema }}."{{ params.dim_table }}_historical"
                WHERE as_of = '{{ ds }}';

            COMMIT;
        """,
        params={
          'dim_table': dim_table,
          'input_schema': input_schema,
          'output_schema': output_schema,
          'fields_sql': fields_sql,
          'select_sql': select_sql,
          'sortkey': sortkey,
        },
        dag=dag,
    )

    write_signal_historical = FBWriteSignalOperator(
        task_id='write_signal_historical',
        conn_id=conn_id,
        schema=output_schema,
        table=dim_table + '_historical',
        dag=dag,
    )
    write_signal_historical.set_upstream(dim_transaction)

    write_signal_view = FBWriteSignalOperator(
        task_id='write_signal_view',
        conn_id=conn_id,
        schema=output_schema,
        table=dim_table,
        dag=dag,
    )
    write_signal_view.set_upstream(dim_transaction)
    return dag
