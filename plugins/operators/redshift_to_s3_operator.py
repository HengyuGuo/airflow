# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class FBS3Hook(S3Hook):
    def get_credentials(self):
        if self._creds_in_config_file:
            a_key, s_key, calling_format = _parse_s3_config(self.s3_config_file,
                                                            self.s3_config_format,
                                                            self.profile)
        elif self._creds_in_conn:
            a_key = self._a_key
            s_key = self._s_key
        return a_key, s_key

class FBRedshiftToS3Transfer(BaseOperator):
    """
    Executes an UNLOAD command to s3 as a CSV with headers
    :param schema: reference to a specific schema in redshift database
    :type schema: string
    :param table: reference to a specific table in redshift database
    :type table: string
    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: string
    :param s3_key: reference to a specific S3 key
    :type s3_key: string
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string
    :param s3_conn_id: reference to a specific S3 connection
    :type s3_conn_id: string
    :param options: reference to a list of UNLOAD options
    :type options: list
    """

    template_fields = (
      'table',
      's3_bucket',
      's3_key',
    )
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            schema,
            table,
            s3_bucket,
            s3_key,
            redshift_conn_id='redshift_default',
            s3_conn_id='s3_default',
            unload_options=tuple(),
            autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(FBRedshiftToS3Transfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.s3_conn_id = s3_conn_id
        self.unload_options = unload_options
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = FBS3Hook(s3_conn_id=self.s3_conn_id)
        a_key, s_key = self.s3.get_credentials()
        unload_options = ('\n\t\t\t').join(self.unload_options)

        unload_query = """
                        UNLOAD ('SELECT * FROM {0}."{1}"')
                        TO 's3://{2}/{3}'
                        with
                        credentials 'aws_access_key_id={4};aws_secret_access_key={5}'
                        {6};
                        """.format(self.schema, self.table,
                                self.s3_bucket, self.s3_key, a_key, s_key, unload_options)

        logging.info('Executing UNLOAD command...')
        self.hook.run(unload_query, self.autocommit)
        logging.info("UNLOAD command complete...")
