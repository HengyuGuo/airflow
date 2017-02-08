# SNS connection strings
SNS_CONNECTION_REGION = 'us-east-1'
FAILURE_SNS_TOPIC = 'arn:aws:sns:us-east-1:950587841421:airflow-failures'
MESSAGE_SNS_TOPIC = 'arn:aws:sns:us-east-1:950587841421:airflow-messages'

# Data types
def _tf_to_bool(x):
    return x == 't'

def _load_json(x):
    import json
    return json.loads(x)

POSTGRES_DATA_TYPES_TO_PYTHON_TYPES = {
    'smallint': int,
    'int2': int,
    'integer': int,
    'int': int,
    'int4': int,
    'bigint': int,
    'int8': int,
    'decimal': float,
    'numeric': float,
    'real': float,
    'float4': float,
    'double precision': float,
    'float8': float,
    'float': float,
    'bool': lambda x: _tf_to_bool(x),
    'boolean': lambda x: _tf_to_bool(x),
    'json': lambda x: _load_json(x),
    'jsonb': lambda x: _load_json(x),
}
