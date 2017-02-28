from collections import OrderedDict

SNOWFLAKE_CONN_ID = 'snowflake'
SNOWFLAKE_ADMIN_CONN_ID = 'snowflake_admin'

S3_BUCKET = 'plp-data-lake'
CSV_STAGE = 'stg_csv'

AIRFLOW_SCHEMA = 'airflow'
STAGING_SCRAPES_SCHEMA = 'staging_scrapes'
SCHEMAS = [
    AIRFLOW_SCHEMA,
    STAGING_SCRAPES_SCHEMA,
]

PASSTHROUGH_TYPE = 'passthru'
BOOLEAN_TYPE = 'BOOLEAN'
DATE_TYPE = 'DATE'
FLOAT_TYPE = 'FLOAT'
JSON_TYPE = 'VARIANT'
INTEGER_TYPE = 'INTEGER'
STRING_TYPE = 'VARCHAR'
TIMESTAMP_WITHOUT_TIME_ZONE_TYPE = 'TIMESTAMP WITHOUT TIME ZONE'
TIMESTAMP_WITH_TIME_ZONE_TYPE = 'TIMESTAMP WITH TIME ZONE'
# Maps postgres types (on the left) to Snowflake types (on the right).
# Note that this is not an exhaustive list. There exist other types
# and they get mapped to STRING_TYPE.
POSTGRES_TO_SNOWFLAKE_DATA_TYPES = {
    'smallint': INTEGER_TYPE,
    'int2': INTEGER_TYPE,
    'integer': INTEGER_TYPE,
    'int': INTEGER_TYPE,
    'int4': INTEGER_TYPE,
    'bigint': INTEGER_TYPE,
    'int8': INTEGER_TYPE,
    'decimal': PASSTHROUGH_TYPE,
    'numeric': PASSTHROUGH_TYPE,
    'real': FLOAT_TYPE,
    'float4': FLOAT_TYPE,
    'double precision': FLOAT_TYPE,
    'float8': FLOAT_TYPE,
    'float': FLOAT_TYPE,
    'boolean': BOOLEAN_TYPE,
    'bool': BOOLEAN_TYPE,
    'char': STRING_TYPE,
    'character': STRING_TYPE,
    'nchar': STRING_TYPE,
    'bpchar': STRING_TYPE,
    'varchar': STRING_TYPE,
    'character varying': STRING_TYPE,
    'nvarchar': STRING_TYPE,
    'text': STRING_TYPE,
    'date': DATE_TYPE,
    'timestamp': TIMESTAMP_WITHOUT_TIME_ZONE_TYPE,
    'timestamp without time zone': TIMESTAMP_WITHOUT_TIME_ZONE_TYPE,
    'timestamptz': TIMESTAMP_WITH_TIME_ZONE_TYPE,
    'timestamp with time zone': TIMESTAMP_WITH_TIME_ZONE_TYPE,
    'json': JSON_TYPE,
    'jsonb': JSON_TYPE,
}

POSTGRES_COLUMNS_WITH_INVALID_DATES = {
    'reflection_log_entries': ['due_on'],
}

# Used in snowflake_set_role_permissions for setting up users.
ROLES_TO_USERS = OrderedDict([
    ('analyst_role', []),
    ('data_eng_role', ['astewart']),
    ('query_tools_role', ['mode', 'tableau']),
    ('workflow_tools_role', ['airflow', 'airflow_admin']),

    # Do not change those below.
    ('ACCOUNTADMIN', ['airflow_admin']),
    ('SYSADMIN', ['airflow_admin']),
])
