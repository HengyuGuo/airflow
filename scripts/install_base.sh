#!/bin/bash

# Note: On linux you might need to run with sudo.
# You may also need to run on linux
#   sudo apt-get build-dep python-psycopg2
#   sudo apt-get install gunicorn

pip install airflow
pip install JayDeBeApi==0.2.0
pip install 'airflow[postgres, s3, jdbc, celery, hive, gcp_api]'
pip install cryptography
pip install flask_bcrypt
