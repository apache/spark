from flux.operators import BashOperator, MySqlOperator, DummyOperator
from flux.models import DAG
from datetime import datetime

default_args = {
    'owner': 'max',
    'start_date': datetime(2014, 9, 1),
    'mysql_dbid': 'local_mysql',
}

dag = DAG(dag_id='example_1')

cmd = 'ls -l'
run_this_last = DummyOperator(
        task_id='run_this_last', 
        **default_args)
dag.add_task(run_this_last)

run_this = BashOperator(
        task_id='run_after_loop', bash_command='echo 1', **default_args)
dag.add_task(run_this)
run_this.set_downstream(run_this_last)

for i in range(5):
    i = str(i)
    task = BashOperator(
            task_id='runme_'+i, 
            bash_command='sleep {{ 10 + macros.random() * 10 }}', 
            **default_args)
    task.set_downstream(run_this)
    dag.add_task(task)

task = BashOperator(
        task_id='also_run_this', bash_command='ls -l', **default_args)
dag.add_task(task)
task.set_downstream(run_this_last)

sql = "CREATE TABLE IF NOT EXISTS deleteme (col INT);"
create_table = MySqlOperator(
        task_id='create_table_mysql', sql=sql, **default_args)
dag.add_task(create_table)

sql = "INSERT INTO deleteme SELECT 1;"
task = MySqlOperator(task_id='also_run_mysql', sql=sql, **default_args)
dag.add_task(task)
task.set_downstream(run_this_last)
task.set_upstream(create_table)
