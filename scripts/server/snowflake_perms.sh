#!/bin/bash

while true; do
  /usr/local/bin/airflow test snowflake_set_table_permissions grant_tables 2016-01-01
done
