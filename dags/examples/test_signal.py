import airflow as airflow
from datetime import datetime

default_args = {
    'owner': 'mistercrunch',
    'start_date': datetime(2014, 8, 21),
    'mysql_dbid': 'local_mysql',
}

dag = airflow.DAG('test_mysql')

create = airflow.operators.MySqlOperator(task_id='create',
        sql='CREATE TABLE IF NOT EXISTS tmp (tmp INT);', **default_args)
dag.add_task(create)

ms = airflow.operators.MySqlSensorOperator(task_id='sensor',
        sql='SELECT COUNT(*) FROM tmp;', **default_args)
dag.add_task(ms)
ms.set_upstream(create)
