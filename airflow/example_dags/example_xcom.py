from __future__ import print_function
import airflow
import datetime

dag = airflow.DAG(
    'example_xcom',
    start_date=datetime.datetime(2015, 1, 1),
    default_args={'owner': 'airflow', 'provide_context': True})

value_1 = [1, 2, 3]
value_2 = {'a': 'b'}
value_3 = set([4, 5, 6])

def push(**kwargs):
    # pushes an XCom without a specific target
    kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)

def push_to_target(**kwargs):
    # pushes an XCom to a target
    kwargs['ti'].xcom_push(
        key='value for puller', value=value_2, to_tasks=['puller'])

def push_by_returning(**kwargs):
    # pushes an XCom without a specific target, just by returning it
    return value_3

def puller(**kwargs):
    ti = kwargs['ti']

    # get value_1 (which must be pulled from the source)
    v1 = ti.xcom_pull(from_tasks=['push'])
    assert v1.iloc[0].value == value_1

    # get value 2 (which was pushed to this task)
    v2 = ti.xcom_pull()
    assert v2.iloc[0].value == value_2

    # get value_3 (which must be pulled from the source)
    v3 = ti.xcom_pull(from_tasks=['push_by_returning'])
    assert v3.iloc[0].value == value_3

push1 = airflow.operators.PythonOperator(
    task_id='push', dag=dag, python_callable=push)

push2 = airflow.operators.PythonOperator(
    task_id='push_to_target', dag=dag, python_callable=push_to_target)

push3 = airflow.operators.PythonOperator(
    task_id='push_by_returning', dag=dag,
    python_callable=push_by_returning)

pull = airflow.operators.PythonOperator(
    task_id='puller', dag=dag, python_callable=puller)

pull.set_upstream([push1, push2, push3])