.. image:: img/pin_large.png
    :width: 70

Airflow's Documentation
================================

Airflow is a system to programmaticaly author, schedule and monitor data pipelines. 

Use the Airflow library to define workflows as directed acyclic graphs (DAGs) of data related tasks. Command line utilities make it easy to run parts of workflows interactively, and commiting pipelines into production is all it takes for the master scheduler to run the pipelines with the schedule and dependencies specified.

The Airflow UI make it easy to visualize pipelines running in production, monitor progress and troubleshoot issues when needed.

Principles
----------

-  **Dynamic:** Airflow has intrinsec support for dynamic pipeline
   generation: you can write pipelines, as well as writing code that
   defines pipeline.
-  **Interactivity:** the libraries are intuitive so that writting /
   testing and monitoring pipelines from a shell (or an iPython
   Notebook) should just flow
-  **Extensible:** easily define your own operators, executors and
   extend the library so that it fits the level of abstraction that
   suits your environment
-  **Elegant:** make your commands dynamic with the power of the
   **Jinja** templating engine

Concepts
~~~~~~~~

Operators
'''''''''

Operators allows to generate a certain type of task on the graph. There
are 3 main type of operators:

-  **Sensor:** Waits for events to happen, it could be a file appearing
   in HDFS, the existance of a Hive partition or for an arbitrary MySQL
   query to return a row.
-  **Remote Execution:** Trigger an operation in a remote system, this
   could be a HQL statement in Hive, a Pig script, a map reduce job, a
   stored procedure in Oracle or a Bash script to run.
-  **Data transfers:** Move data from a system to another. Push data
   from Hive to MySQL, from a local file to HDFS, from Postgres to
   Oracle, or anything of that nature.

Tasks
'''''

A task represent the instantiation of an operator and becomes a node in
the directed acyclic graph (DAG). The instantiation defines specific
values when calling the abstract operator. A task could be waiting for a
specific partition in Hive, or triggerring a specific DML statement in
Oracle.

Task instances
''''''''''''''

A task instance represents a task run, for a specific point in time.
While the task defines a start datetime and a schedule (say every hour
or every day), a task instance represents a specific run of a task. A
task instance will have a status of either "started", "retrying",
"failed" or "success"

Content
-------
.. toctree::
    :maxdepth: 4

    installation
    code
    cli
    ui
    tutorial
