#!/bin/bash

# Make sure you have homebrew installed:
# See http://brew.sh/

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
brew install python
# Need latest java for snowflake.
brew cask install java
$DIR/install_base.sh
echo 'export JAVA_HOME=$(/usr/libexec/java_home)' >> ~/.bash_profile
echo 'export AIRFLOW_HOME=$HOME/airflow' >> ~/.bash_profile
echo 'export CLASSPATH="$CLASSPATH:$HOME/airflow/java/snowflake-jdbc-3.0.3.jar"' >> ~/.bash_profile
