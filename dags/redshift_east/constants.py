REDSHIFT_CONN_ID='redshift_east'
STAGING_SCRAPES_SCHEMA='airflow_staging_scrapes'
# Note: In matillion (prod), the schema for these tables is "public".
# We use a different schema to prevent clobbering the production job.
DIM_AND_FCT_SCHEMA='airflow_dim_tables'
