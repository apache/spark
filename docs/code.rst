Code / API
==========

Models
------
Models are built on top of th SQLAlchemy ORM Base class, instance are
persisted in the database.

.. automodule:: airflow.models
    :show-inheritance:
    :members: DAG, BaseOperator, TaskInstance, DagBag, Connection

Hooks
-----
.. automodule:: airflow.hooks
    :show-inheritance:
    :members: MySqlHook, PrestoHook, HiveHook

Operators
---------
Operators allows to generate a certain type of task on the graph.

.. automodule:: airflow.operators
    :show-inheritance:
    :members: MySqlOperator, BashOperator, ExternalTaskSensor, HiveOperator, SqlSensor, HivePartitionSensor, EmailOperator 

Executors
---------
Executors are the mechanism by which task instances get run. 
.. automodule:: airflow.executors
    :show-inheritance: 
    :members: LocalExecutor, CeleryExecutor, SequentialExecutor
