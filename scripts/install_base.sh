#!/bin/bash

# Note: On linux you might need to run with sudo.
# You may also need to run sudo apt-get build-dep python-psycopg2

pip install airflow
pip install 'airflow[postgres, s3, jdbc, celery]'
echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bash_profile
echo 'export JAVA_HOME=$(/usr/libexec/java_home)' >> ~/.bash_profile
echo 'export CLASSPATH="$CLASSPATH:$HOME/airflow/java/snowflake-jdbc-3.0.3.jar"' >> ~/.bash_profile
