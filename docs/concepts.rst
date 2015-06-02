Concepts
========

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

Task Instances
''''''''''''''

A task instance represents a task run, for a specific point in time.
While the task defines a start datetime and a schedule (say every hour
or every day), a task instance represents a specific run of a task. A
task instance will have a status of either "started", "retrying",
"failed" or "success"

Hooks
'''''

Hooks are interfaces to external platforms and databases like Hive, S3, MySQL,
Postgres, HDFS, Pig and Cascading. Hooks implement a common interface when
possible, and act as a building block for operators. They also use 
the ``airflow.connection.Connection`` model to retrieve hostnames
and authentication information. Hooks keeps authentication code and 
information out of pipelines, centralized in the metadata database.

Hooks are also very useful on their own to use in Python scripts, 
Airflow airflow.operators.PythonOperator, and in interactive environment
like iPython or Jupyter Notebook.

Pools
'''''

Some systems can get overwelmed when too many processes hit them at the same
time. Airflow pools can be used to **limit the execution parallelism** on 
arbitrary sets of tasks. The list of pools is managed in the UI 
(``Menu -> Admin -> Pools``) by giving the pools a name and assigning 
it a number of worker slots. Tasks can then be associated with 
one of the existing pools by using the ``pool`` parameter when 
creating tasks (aka instantiating operators).

Tasks will be scheduled as usual while the slots fill up. Once capacity is
reached, runnable tasks get queued and there state will show as such in the
UI. As slots free up, queued up tasks start running.

Note that by default tasks aren't assigned to any pool and their 
execution parallelism is only limited to the executor's setting.
