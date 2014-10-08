from flux.operators import BashOperator
from flux.models import DAG
from datetime import datetime

default_args = {
    'owner': 'mistercrunch',
    'start_date': datetime(2014, 9, 1),
}

dag = DAG(dag_id='example_2')

cmd = 'ls -l'
run_this_last = BashOperator(task_id='run_this_last', bash_command='echo 1', **default_args)
dag.add_task(run_this_last)

run_this = BashOperator(task_id='run_this', bash_command='echo 1', **default_args)
dag.add_task(run_this)
run_this.set_downstream(run_this_last)

for i in range(10):
    i = str(i)
    task = BashOperator(
            task_id='runme_'+i, 
            bash_command='echo "'+str(i)+': {{ ti.execution_date }}"', 
            **default_args)
    task.set_downstream(run_this)
    dag.add_task(task)

task = BashOperator(task_id='also_run_this', bash_command='ls -l', **default_args)
dag.add_task(task)
task.set_downstream(run_this_last)

#dag.tree_view()
#dag.db_merge()
#dag.run()
