# Globally relevant constants go here. Warehouse and scrapes specific constants
# go in dags/redshift/constants.py, dags/scrapes/constants.py, dags/snowflake/constants.py, etc.

# Most jobs should start at 3am, after the DB scrapes are done.
DEFAULT_SCHEDULE_INTERVAL = '0 3 * * *'

YESTERDAY_MACRO = '{{ macros.ds_add(ds, -1) }}'
TOMORROW_MACRO = '{{ macros.ds_add(ds, 1) }}'
