from datetime import date
import json
import os
import tempfile

from redshift.constants import SLAVE_DB_CONN_ID
from airflow.hooks.base_hook import BaseHook

TMP_DIR = 'airflow-cache/'

"""
In the rare case that a DB query needs to be done in the
course of DAG definition, we can reduce the loading cost
of DAGs by caching that query. A good example is the
nightly scrapes pipeline which creates a SubDagOperator
for each of the tables in the database.
"""
class FBCachedDbApiHook(BaseHook):
    def __init__(self, conn_id):
        self.conn_id = conn_id

    def get_records(self, key, sql):
        tmp_path = self.tmp_path(key)
        # Attempt read from cache
        if os.path.isfile(tmp_path):
            with open(tmp_path, 'r') as cache_file:
                cache_json = cache_file.read()
            return json.loads(cache_json)

        records = self.get_connection(self.conn_id).get_hook().get_records(sql)
        # Write back to cache
        if not os.path.exists(self.tmp_dir()):
             os.mkdir(self.tmp_dir())
        with open(tmp_path, 'w+b') as cache_file:
             cache_file.write(json.dumps(records))

        return records

    def tmp_dir(self):
        return tempfile.gettempdir() + '/' + TMP_DIR

    def tmp_path(self, key):
        return self.tmp_dir() + key

    @classmethod
    def gen_postgres_tables(cls):
        hook = cls(conn_id=SLAVE_DB_CONN_ID)
        key = 'gen_postgres_tables_' + str(date.today())
        records = hook.get_records(
            key=key,
            sql="""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """,
        )
        for record in records:
            yield record[0]
