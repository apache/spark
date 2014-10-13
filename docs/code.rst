Code / API
==========

Models
------
Models are built on top of th SQLAlchemy ORM Base class, instance are
persisted in the database.

.. automodule:: models
    :members: DAG, BaseOperator, TaskInstance, DagBag, DatabaseConnection

Operators
---------
Operators allows to generate a certain type of task on the graph.

.. automodule:: operators
    :members: MySqlOperator, BashOperator, MySqlSensorOperator, ExternalTaskSensor

Hooks
-----
.. automodule:: hooks
    :members: MySqlHook

Executors
---------
.. automodule:: executors
    :members: LocalExecutor, SequentialExecutor
