from airflow import DAG
from airflow.operators import PythonOperator
from airflow.contrib.operators import QuboleOperator
from datetime import datetime, timedelta
import filecmp

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'schedule_interval': timedelta(1),
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('example_qubole_operator', default_args=default_args)

def compare_result(ds, **kwargs):
    r1 = t1.get_results(None, True)
    r2 = t2.get_results(None, True)
    return filecmp.cmp(r1, r2)

# t1, t2 and t3 are examples of tasks created by instatiating operators
t1 = QuboleOperator(
    task_id='hive_show_table',
    command_type='hivecmd',
    query='show tables',
    cluster_label='default',
    fetch_logs=True,
    tags='aiflow_example_run',
    dag=dag)

t2 = QuboleOperator(
    task_id='hive_s3_location',
    command_type="hivecmd",
    script_location="s3n://dev.canopydata.com/airflow/show_table.hql",
    notfiy=True,
    tags='aiflow_example_run',
    dag=dag)

t3 = PythonOperator(
    task_id='compare_result',
    provide_context=True,
    python_callable=compare_result,
    dag=dag)

t3.set_upstream(t1)
t3.set_upstream(t2)
