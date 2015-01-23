from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime

args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 1, 1),
    'mysql_dbid': 'local_mysql',
}

dag = DAG(dag_id='example3')

run_this = BashOperator(
    task_id='also_run_this', bash_command='ls -l', default_args=args)
dag.add_task(run_this)

for i in range(5):
    i = str(i)
    task = BashOperator(
        task_id='runme_'+i,
        bash_command='sleep {{ 10 + macros.random() * 10 }}',
        default_args=args)
    task.set_upstream(run_this)
    dag.add_task(task)
