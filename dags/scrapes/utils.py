from scrapes.constants import S3_KEY_PREFIX

def get_data_s3_key(table_name, as_of='{{ ds }}'):
    return '{}/{}/{}.tsv.gz'.format(S3_KEY_PREFIX, table_name, as_of)

def get_schema_s3_key(table_name, as_of='{{ ds }}'):
    return '{}/{}/schemata/{}.json'.format(S3_KEY_PREFIX, table_name, as_of)

def get_json_s3_key(table_name, as_of='{{ ds }}'):
    return '{}/{}/{}.json.gz'.format(S3_KEY_PREFIX, table_name, as_of)
