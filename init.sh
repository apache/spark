#!/bin/bash
source $AIRFLOW_HOME/env/bin/activate
export PYTHONPATH=$AIRFLOW_HOME
export AIRFLOW_CONFIG_PATH=$AIRFLOW_HOME/airflow/airflow.cfg

# Create the conf file from template if it doesn't exist
if [ ! -f $AIRFLOW_CONFIG_PATH ]; then
    sed "s#TO_REPLACE_FROM_OS_ENVIRON#$AIRFLOW_HOME#" ${AIRFLOW_CONFIG_PATH}.template > $AIRFLOW_CONFIG_PATH
fi
