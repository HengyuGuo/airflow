import csv
import json
import logging
import subprocess
import sys
import tempfile

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from fb_required_args import require_keyword_args

from operators.constants import POSTGRES_DATA_TYPES_TO_PYTHON_TYPES

class FBCSVToJSONOperator(BaseOperator):
    template_fields = (
      'data_s3_key',
      'schema_s3_key',
      'json_s3_key',
    )

    @apply_defaults
    @require_keyword_args(['task_id', 'data_s3_key', 'schema_s3_key', 'json_s3_key', 'dag'])
    def __init__(
            self,
            s3_conn_id='s3_default',
            replace=True,
            *args, **kwargs):
        self.data_s3_key = kwargs['data_s3_key']
        self.schema_s3_key = kwargs['schema_s3_key']
        self.json_s3_key = kwargs['json_s3_key']
        self.s3_conn_id = s3_conn_id
        self.replace = replace

        del kwargs['data_s3_key']
        del kwargs['schema_s3_key']
        del kwargs['json_s3_key']
        super(FBCSVToJSONOperator, self).__init__(*args, **kwargs)

    def _jsonify_row_with_types(self, row, schema_array):
        # Convert types
        for schema in schema_array:
            if '\\N' in row[schema[0]]:
                # Just set NULL values to None
                row[schema[0]] = None
            elif schema[1] in POSTGRES_DATA_TYPES_TO_PYTHON_TYPES:
                row[schema[0]] = POSTGRES_DATA_TYPES_TO_PYTHON_TYPES[schema[1]](row[schema[0]])
            else:
                # Keep everything, including date/time/character/text/arrays/etc., as a string
                continue

        return json.dumps(row)

    def execute(self, context):
        self.s3 = S3Hook(s3_conn_id=self.s3_conn_id)

        with tempfile.NamedTemporaryFile('w+b') as tmp_file:
            logging.info('Reading from s3: ' + self.schema_s3_key)
            schema_key = self.s3.get_key(self.schema_s3_key)
            schema_array = json.loads(schema_key.get_contents_as_string())

            logging.info('Reading from s3: ' + self.data_s3_key)
            data_key = self.s3.get_key(self.data_s3_key)
            unzip_proc = subprocess.Popen(
                'gunzip',
                stdin=subprocess.PIPE,
                stdout=tmp_file,
                shell=True,
            )
            data_key.get_file(unzip_proc.stdin)
            unzip_proc.communicate()

            reopen = open(tmp_file.name, 'rb')
            csv.field_size_limit(sys.maxsize)
            reader = csv.DictReader(
                reopen,
                fieldnames=[col[0] for col in schema_array],
                dialect='excel-tab',
            )
            with tempfile.NamedTemporaryFile('w+b') as json_tmp_file:
                logging.info('Parsing CSV into JSON ...')
                zip_proc = subprocess.Popen(
                    'gzip',
                    stdin=subprocess.PIPE,
                    stdout=json_tmp_file,
                    shell=True,
                )

                first_line = True
                for row in reader:
                    # Sometimes you just wish python had do while loops
                    if not first_line:
                        zip_proc.stdin.write('\n')

                    zip_proc.stdin.write(self._jsonify_row_with_types(row, schema_array))
                    first_line = False

                zip_proc.communicate()

                logging.info('Writing to s3: ' + self.json_s3_key)
                self.s3.load_file(
                    filename=json_tmp_file.name,
                    key=self.json_s3_key,
                    replace=self.replace,
                )
