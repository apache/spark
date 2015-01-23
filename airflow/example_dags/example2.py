from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime

args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 1, 1),
    'depends_on_past': True,
}

dag = DAG(dag_id='example2')

cmd = 'ls -l'
run_this_last = BashOperator(
    task_id='run_this_last', bash_command='echo 1', default_args=args)
dag.add_task(run_this_last)

run_this = BashOperator(
    task_id='run_this', bash_command='echo 1', default_args=args)
dag.add_task(run_this)
run_this.set_downstream(run_this_last)

for i in range(10):
    i = str(i)
    task = BashOperator(
        task_id='runme_'+i,
        bash_command='sleep 10',
        default_args=args)
    task.set_downstream(run_this)
    dag.add_task(task)

cmd = "echo {{ params.tables.the_table }}"
task = BashOperator(
    task_id='also_run_this', bash_command=cmd,
    params={
        'tables': {
            'the_table': 'da_table',
        }
    },
    default_args=args)
dag.add_task(task)
task.set_downstream(run_this_last)
task.set_upstream(run_this)
