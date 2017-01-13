REDSHIFT_CONN_ID='redshift_east'
REDSHIFT_ADMIN_CONN_ID='redshift_east_admin'
STAGING_SCRAPES_SCHEMA='airflow_staging_scrapes'
# Note: In matillion (prod), the schema for these tables is "public".
# We use a different schema to prevent clobbering the production job.
DIM_AND_FCT_SCHEMA='airflow_dim_tables'
YESTERDAY_MACRO='{{ macros.ds_add(ds, -1) }}'
