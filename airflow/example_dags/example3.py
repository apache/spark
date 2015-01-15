from airflow.operators import BashOperator, MySqlOperator
from airflow.models import DAG
from datetime import datetime

default_args = {
    'owner': 'max',
    'start_date': datetime(2014, 9, 1),
    'mysql_dbid': 'local_mysql',
}

dag = DAG(dag_id='example_3')

run_this = BashOperator(
        task_id='also_run_this', bash_command='ls -l', **default_args)
dag.add_task(run_this)

for i in range(5):
    i = str(i)
    task = BashOperator(
            task_id='runme_'+i, 
            bash_command='sleep {{ 10 + macros.random() * 10 }}', 
            **default_args)
    task.set_upstream(run_this)
    dag.add_task(task)

