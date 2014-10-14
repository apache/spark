from flux import DAG
from datetime import datetime, timedelta
from flux.operators import BashOperator

default_args = {
    'owner': 'max',
    'start_date': datetime(2014, 10, 12),
    'schedule_interval': timedelta(hours=1),
    'mysql_dbid': 'local_mysql',
}

dag = DAG('hourly', schedule_interval=timedelta(hours=1))

task1 = BashOperator(
    task_id="bash_echo", 
    bash_command="echo {{ ti.execution_date }}", **default_args)
dag.add_task(task1)

task2 = BashOperator(
    task_id="bash_sleep5", 
    bash_command="sleep 5", **default_args)
dag.add_task(task2)
