from datetime import datetime
default_args = {
    'owner': 'max',
    'start_date': datetime(2014, 9, 1),
    'mysql_dbid': 'local_mysql',
}
from airflow.operators import HivePartitionSensor
from airflow import settings
from airflow import DAG

dag = DAG("test_wfh")
t = HivePartitionSensor(task_id="test_hps", schema='core_data',table="fct_revenue", partition="ds='{{ ds }}'", **default_args )
dag.add_task(t)
