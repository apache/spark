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
    :members: 
        HiveOperator, 
        MySqlOperator, 
        BashOperator, 
        PythonOperator, 
        ExternalTaskSensor, 
        SqlSensor, 
        HivePartitionSensor, 
        HdfsSensor,
        PrestoCheckOperator,
        PrestoIntervalCheckOperator,
        PrestoValueCheckOperator,
        Hive2SambaOperator,
        DummyOperator,
        EmailOperator

Macros
---------
Here's a list of variables and macros that can be used in templates


Default Variables
'''''''''''''''''
The Airflow engine passes a few variables by default that are accessible
in all templates

=================================   ====================================
Variable                            Description
=================================   ====================================
``{{ ds }}``                        the execution date as ``YYYY-MM-DD``
``{{ execution_date }}``            the execution_date, (datateime.datetime)
``{{ dag }}``                       the DAG object
``{{ task }}``                      the Task object
``{{ macros }}``                    a reference to the macros package, described bellow
``{{ task_instance }}``             the task_instance object
``{{ ds_nodash }}``                 the execution date as ``YYYYMMDD``
``{{ end_date }}``                  same as ``{{ ds }}``
``{{ lastest_date }}``              same as ``{{ ds }}``
``{{ ti }}``                        same as ``{{ task_instance }}``
``{{ params }}``                    a reference to the user defined params dictionary
``{{ task_instance_key_str }}``     a unique, human readable key to the task instance
                                    formatted ``{dag_id}_{task_id}_{ds}``
=================================   ====================================

Note that you can access the objects attributes and methods with simple
dot notation. Here are some examples of what is possible: 
``{{ task.owner }}``, ``{{ task.task_id }}``, ``{{ ti.hostname }}``, ...
Refer to the models documentation for more information on the objects
attributes and methods.

Macros
''''''
These macros live under the ``macros`` namespace in your templates.

.. automodule:: airflow.macros
    :show-inheritance: 
    :members:

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
