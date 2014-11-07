#!/bin/bash
source $AIRFLOW_HOME/env/bin/activate
export PYTHONPATH=$AIRFLOW_HOME
export AIRFLOW_CONFIG_PATH=$AIRFLOW_HOME/airflow/airflow.cfg
sed -i .bk "s#TO_REPLACE_FROM_OS_ENVIRON#$AIRFLOW_HOME#" $AIRFLOW_CONFIG_PATH
