
Installation
------------

Quick Start
'''''''''''
The installation is quick and straightforward. 

::

    # airflow needs a home, ~/airflow is the default, 
    # but you can lay foundation somewhere else if you prefer
    export AIRFLOW_HOME=~/airflow

    # install from pypi using pip
    pip install airflow

    # initialize the database
    airflow initdb

    # start the web server, default port is 8080
    airflow webserver -p 8080

Upon running these commands, airflow will create the $AIRFLOW_HOME folder and
lay an "airflow.cfg" files with defaults that get you going fast. You can
inspect the file either in $AIRFLOW_HOME/airflow.cfg, or through the UI in 
the Admin->Configuration menu.

Out of the box, airflow uses a sqlite database, which you should outgrow 
fairly quickly since no parallelization is possible using this database
backend. It works in conjunction with the "SequentialExecutor" which will 
only run task instances sequentially. While this is very limiting, it allows
you to get up and running quickly and take a tour of the UI and the 
command line utilities.

Here are a few commands that will trigger a few task instances. You should
be able to see the status of the jobs change in the example_1 DAG as you 
run the commands below.

::

    # run your first task instance
    airflow run example_1 runme_0 2015-01-01
    # run a backfill over 2 days
    airflow backfill example_1 -s 2015-01-01 -e 2015-01-02


Setting up a Backend
''''''''''''''''''''
If you want to take a real test drive of Airflow, you should consider 
setting up a real database backend and switching to the LocalExecutor.

As Airflow was built to interact with its metadata using the great SqlAlchemy
library, you should be able to use any database backend supported as a
SqlAlchemy backend. We recommend using **MySQL** or **Postgres**.

Once you've setup your database to host Airflow, you'll need to alter the
SqlAlchemy connection string located in your airflow.cfg 
($AIRFLOW_HOME/airflow.cfg). You should then also change the "executor" 
setting to use "LocalExecutor", an executor that can parallelize task
instances locally.

::

    # initialize the database
    airflow initdb

Connections
'''''''''''
Airflow needs to know how to connect to your environment. Information 
such as hostname, port, login and password to other systems and services is
handled Admin->Connection section of the UI. The pipeline code you will 
author will reference the 'conn_id' of the Connection objects.

.. image:: img/connections.png


Scaling Out
'''''''''''
CeleryExecutor is the way you can scale out the number of workers. For this
to work, you need to setup a Celery backend (RabbitMQ, Redis, ...) and
change your airflow.cfg to point the executor parameter to 
CeleryExecutor and provide the related Celery settings.

To kick off a worker, you need to setup Airflow and quick off the worker 
subcommand

::

    airflow worker

Your worker should start picking up tasks as soon as they get fired up in
its direction.

Note that you can also run "Celery Flower" a web UI build on top of Celery
to monitor your workers.
