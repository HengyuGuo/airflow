#!/bin/bash

if [ $(whoami) != 'root' ]; then
  echo 'You need to run this with sudo. Run: sudo scripts/install_ec2.sh'
  exit 1
fi

sudo apt-get install arcanist
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
$DIR/install_base.sh
echo 'export AIRFLOW_HOME=$HOME/airflow' >> ~/.bashrc
echo 'export CLASSPATH="$CLASSPATH:$HOME/airflow/java/snowflake-jdbc-3.0.3.jar"' >> ~/.bashrc
