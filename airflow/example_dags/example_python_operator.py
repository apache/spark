from airflow.operators import PythonOperator
from airflow.models import DAG
from datetime import datetime

import time
from pprint import pprint

args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 1, 1),
}

dag = DAG(dag_id='example_python_operator')

def my_sleeping_function(random_base):
    '''This is a function that will run whithin the DAG execution'''
    time.sleep(random_base)

def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    default_args=args)
dag.add_task(run_this)

for i in range(10):
    '''
    Generating 10 sleeping task, sleeping from 0 to 9 seconds
    respectively
    '''
    task = PythonOperator(
        task_id='sleep_for_'+str(i),
        python_callable=my_sleeping_function,
        op_kwargs={'random_base': i},
        default_args=args)

    task.set_upstream(run_this)
    dag.add_task(task)
