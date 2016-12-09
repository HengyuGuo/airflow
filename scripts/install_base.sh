#!/bin/bash

# Note: On linux you might need to run with sudo.
# You may also need to run on linux
#   sudo apt-get build-dep python-psycopg2
#   sudo apt-get install gunicorn

pip install airflow
pip install 'airflow[postgres, s3, jdbc, celery, hive]'
pip install cryptography
echo 'export AIRFLOW_HOME=$HOME/airflow' >> ~/.bash_profile
echo 'export CLASSPATH="$CLASSPATH:$HOME/airflow/java/snowflake-jdbc-3.0.3.jar"' >> ~/.bash_profile
