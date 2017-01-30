"""
This file defines constants shared between different airflow jobs.
"""

# Connection IDs
REDSHIFT_CONN_ID = 'redshift_east'
REDSHIFT_ADMIN_CONN_ID = 'redshift_east_admin'
SLAVE_DB_CONN_ID = 'postgres_heroku_slave'

# Schema names
HEROKU_PUBLIC_SCHEMA = 'airflow_heroku_public'
STAGING_SCRAPES_SCHEMA = 'airflow_staging_scrapes'
# Note: In matillion (prod), the schema for these tables is "public".
# We use a different schema to prevent clobbering the production job.
DIM_AND_FCT_SCHEMA = 'airflow_dim_tables'

# Handy macros
YESTERDAY_MACRO = '{{ macros.ds_add(ds, -1) }}'
TOMORROW_MACRO = '{{ macros.ds_add(ds, 1) }}'
