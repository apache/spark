Code / API
==========

Operators
---------
Operators allows to generate a certain type of task that become a node in
the DAG when instantiated. All operators derive from BaseOperator and
inherit a whole lot of attributes and method that way. Refer to the 
BaseOperator documentation for more details.

.. automodule:: airflow.operators
    :show-inheritance:
    :members: MySqlOperator, BashOperator, ExternalTaskSensor, HiveOperator, 
        SqlSensor, HivePartitionSensor, EmailOperator, PrestoCheckOperator

Macros
---------
Sweet macros that can be used in your templates

.. automodule:: airflow.macros.hive
    :show-inheritance: 
    :members:

.. _models_ref:

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

Executors
---------
Executors are the mechanism by which task instances get run. 

.. automodule:: airflow.executors
    :show-inheritance: 
    :members: LocalExecutor, CeleryExecutor, SequentialExecutor
