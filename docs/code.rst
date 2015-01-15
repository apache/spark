Code / API
==========

Models
------
Models are built on top of th SQLAlchemy ORM Base class, instance are
persisted in the database.

.. automodule:: models
    :show-inheritance:
    :members: DAG, BaseOperator, TaskInstance, DagBag, DatabaseConnection

Operators
---------
Operators allows to generate a certain type of task on the graph.

.. automodule:: operators
    :show-inheritance:
    :members: MySqlOperator, BashOperator, ExternalTaskSensor, HiveOperator, SqlSensor, HivePartitionSensor

Hooks
-----
.. automodule:: hooks
    :show-inheritance:
    :members: MySqlHook

Executors
---------
.. automodule:: executors
    :show-inheritance:
    :members: LocalExecutor, SequentialExecutor
