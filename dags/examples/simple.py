from airflow.operators import MySqlOperator
from airflow.executors import SequentialExecutor
from airflow import DAG
from datetime import datetime

# Setting some default operator parameters
default_args = {
            'owner': 'max',
                'mysql_dbid': 'local_mysql',
                }

# Initializing a directed acyclic graph
dag = DAG(dag_id='simple', )

# MySQL Operator
sql = "TRUNCATE TABLE tmp;"
mysql_fisrt = MySqlOperator(task_id='mysql_fisrt', sql=sql, **default_args)
dag.add_task(mysql_fisrt)

sql = """
INSERT INTO tmp
SELECT {{ macros.random() * 100 }};
"""
mysql_second = MySqlOperator(task_id='mysql_second', sql=sql, **default_args)
dag.add_task(mysql_second)
mysql_second.set_upstream(mysql_fisrt)

#dag.tree_view()

#dag.run(start_date=datetime(2014, 9, 1), end_date=datetime(2014, 9, 1))

