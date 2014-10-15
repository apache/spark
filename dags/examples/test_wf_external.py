from airflow.operators import ExternalTaskSensor
from airflow import DAG
from datetime import datetime

# Setting some default operator parameters
default_args = {
    'owner': 'max',
    'mysql_dbid': 'local_mysql',
}

# Initializing a directed acyclic graph
dag = DAG(dag_id='test_wf')

wf_ext = ExternalTaskSensor(
    task_id='wf_ext', 
    external_dag_id='example_1', external_task_id='runme_0', **default_args)
dag.add_task(wf_ext)
