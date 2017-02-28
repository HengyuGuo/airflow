"""
This file defines constants shared between different airflow jobs.
"""

# Connection IDs
REDSHIFT_CONN_ID = 'redshift_east'
REDSHIFT_ADMIN_CONN_ID = 'redshift_east_admin'
REDSHIFT_NON_ETL_CONN_ID = 'redshift_east_nonetl' # Only used to simulate non-ETL user, like mode
SLAVE_DB_CONN_ID = 'postgres_heroku_slave'

# Schema names
HEROKU_PUBLIC_SCHEMA = 'airflow_heroku_public'
STAGING_SCRAPES_SCHEMA = 'staging_scrapes'
STAGING_SCRAPES_WRITE_SCHEMA = 'airflow_staging_scrapes'
DIM_AND_FCT_SCHEMA = 'public'

# Legal Redshift data types
REDSHIFT_DATA_TYPES = [
    'smallint',
    'int2',
    'integer',
    'int',
    'int4',
    'bigint',
    'int8',
    'decimal',
    'numeric',
    'real',
    'float4',
    'double precision',
    'float8',
    'float',
    'boolean',
    'bool',
    'char',
    'character'
    'nchar',
    'bpchar',
    'varchar',
    'character varying',
    'nvarchar',
    'text',
    'date'
    'timestamp',
    'timestamp without time zone',
    'timestamptz',
    'timestamp with time zone',
]
MAX_VARCHAR_COLUMN_LENGTH = 65535
