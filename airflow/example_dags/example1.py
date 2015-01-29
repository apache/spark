from airflow.operators import BashOperator, DummyOperator
from airflow.models import DAG
from datetime import datetime

args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 1, 1),
}

dag = DAG(dag_id='example1')

cmd = 'ls -l'
run_this_last = DummyOperator(
    task_id='run_this_last',
    default_args=args)
dag.add_task(run_this_last)

run_this = BashOperator(
    task_id='run_after_loop', bash_command='echo 1',
    default_args=args)
dag.add_task(run_this)
run_this.set_downstream(run_this_last)
for i in range(9):
    i = str(i)
    task = BashOperator(
        task_id='runme_'+i,
        bash_command='echo "{{ task_instance_key_str }}" && sleep 60',
        default_args=args)
    task.set_downstream(run_this)
    dag.add_task(task)

task = BashOperator(
    task_id='also_run_this',
    bash_command='echo "{{ macros.uuid.uuid1() }}"',
    default_args=args)
dag.add_task(task)
task.set_downstream(run_this_last)
