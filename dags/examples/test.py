from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'mistercrunch',
    'start_date': datetime(2014, 9, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'email': "maxime.beauchemin@airbnb.com",
    'email_on_retry': True,
    'email_on_failure': True,
}

dag = DAG(dag_id='test_dag')

cmd = 'ls -l'
task1 = BashOperator(task_id='always_fail', bash_command='typo', **default_args)
dag.add_task(task1)

task2 = BashOperator(task_id='step2', bash_command='echo 2', **default_args)
task2.set_downstream(task1)
dag.add_task(task2)

task3 = BashOperator(task_id='step3', bash_command='ls -l', **default_args)
task3.set_downstream(task1)
dag.add_task(task3)

task4 = BashOperator(task_id='step4', bash_command='ls -l', **default_args)
task4.set_downstream(task3)
dag.add_task(task4)

