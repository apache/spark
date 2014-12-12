import airflow as airflow
from datetime import datetime
from airflow import operators

default_args = {
    'owner': 'mistercrunch',
    'start_date': datetime(2014, 8, 21),
    'mysql_dbid': 'local_mysql',
    'email_on_failure': True,
}

dag = airflow.DAG('test_mysql')

create = operators.MySqlOperator(task_id='create',
        sql='CREATE TABLE IF NOT EXISTS tmp (tmp INT);', **default_args)
dag.add_task(create)

ms = operators.SqlSensor(task_id='sensor', db_id=default_args['mysql_dbid'],
        sql='SELECT COUNT(*) FROM tmp;', **default_args)
dag.add_task(ms)
ms.set_upstream(create)
