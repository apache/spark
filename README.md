Flux
====
Flux is a system to programmaticaly author, schedule and monitor data pipelines. 

Datanauts write Python that define workflows as directed acyclic graphs (DAGs) of data related tasks. They can run tasks and workflow for different timeframe interactively, and commiting their pipelines into production is all it takes for the master scheduler to run the pipelines with the schedule and dependencies specified.

The Flux UI make it easy to visualize the pipelines running in production, their hierachy, progress and exposes way to pinpoint and troubleshoot issues when needed.

### Principles
* **Interactivity:** the libraries are intuitive so that writting / testing and monitoring pipelines from a shell (or an iPython Notebook) should just flow
* **Dynamic:** Flux has intrinsec support for dynamic pipeline generation: you can write pipelines, as well as writing code that defines pipeline.
* **Extensible:** easily define your own operators, executors and extend the library so that it fits the level of abstraction that suits your environment
* **Elegant:** make your commands dynamic with the power of the **Jinja** templating engine
* **Distributed:** Distributed scheduling, workers trigger downstream jobs, a job supervisor triggers tasks instances, an hypervisor monitors jobs and restart them when no heartbeats are detected.

### Concepts
##### Operators
An operator allows to generate a certain type of task on the graph. There are 3 main type of operators:

* **Sensor:** Waits for events to happen, it could be a file appearing in HDFS, the existance of a Hive partition or for an arbitrary MySQL query to return a row.
* **Remote Execution:** Trigger an operation in a remote system, this could be a HQL statement in Hive, a Pig script, a map reduce job, a stored procedure in Oracle or a Bash script to run.
* **Data transfers:** Move data from a system to another. Push data from Hive to MySQL, from a local file to HDFS, from Postgres to Oracle, or anything of that nature.
##### Tasks
A task represent the instantiation of an operator and becomes a node in the directed acyclic graph (DAG). The instantiation defines specific values when calling the abstract operator. A task would be waiting for a specific partition in Hive, or triggerring a specific DML statement in Oracle.
##### Task instances
A task instance represents a task run, for a specific point in time. While the task defines a start datetime and a schedule (say every hour or every day), a task instance represents a specific run of a task. A task instance will have a status of either "started", "retrying", "failed" or "success"

Installation
------------
##### Debian packages
	sudo apt-get install virtualenv python-dev
	sudo apt-get install libmysqlclient-dev mysql-server
	sudo apt-get g++
##### Create a python virtualenv
	virtualenv env # creates the environment
	source init.sh # activates the environment
##### Use pip to install the python packages required by Flux
	pip install -r requirements.txt
##### Setup the metdata database
Here are steps to get started using MySQL as a backend for the metadata database, though any backend supported by SqlAlquemy should work just fine.

    $ mysql -u root -p 
    mysql> CREATE DATABASE flux;
    CREATE USER 'flux'@'localhost' IDENTIFIED BY 'flux';
    GRANT ALL PRIVILEGES ON flux.* TO 'flux'@'localhost';

TODO
-----
#### UI
* Tree view: remove dummy root node
* Graph view add tooltip
* Backfill wizard
* Fix datepicker

#### Command line
* Add support for including upstream and downstream
#### Write unittests
* For each existing operator
#### More Operators!
* HIVE
* BaseDataTransferOperator
* File2MySqlOperator
* PythonOperator
* DagTaskSensor for cross dag dependencies
* PIG
#### Macros
* Hive latest partition
* Previous execution timestamp
* ...
#### Backend
* LocalExecutor, ctrl-c should result in error state instead of forever running
* CeleryExecutor
* Clear should kill running jobs
#### Misc
* BaseJob
    * DagBackfillJob
    * TaskIntanceJob
    * ClearJob?
* Write an hypervisor, looks for dead jobs without a heartbeat and kills
* Authentication with Flask-Login and Flask-Principal
* email_on_retry

#### Testing required
* result queue with remove descendants

#### Wishlist
* Jobs can send pickles of local version to remote executors?
* Support for cron like synthax (0 * * * ) using croniter library
