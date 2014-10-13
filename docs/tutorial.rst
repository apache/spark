
Flux tutorial
=============

This tutorial walks through some of the fundamental concepts, objects
and their usage.

Hooks
-----

Hooks are a simple abstraction layer on systems Flux interacts with. You
should expect a lot more consistency across hooks than you would with
the different systems' underlying libraries. You should also expect a
higher level of abstraction.

Connection information is stored in the Flux metadata database, so that
you don't need to hard code or remember this connection information. In
the bellow example, we connect to a MySQL database by specifying the
mysql\_dbid, which looks up Flux's metadata to get the actual hostname,
login, password, and schema name behind the scene.

Common methods: \* Get a recordset \* Extract a csv file \* Run a
statement \* Load into a table from a csv file \* Get a pandas dataframe
\* Get a json blob (array of objects)

.. code:: python

    # A little bit of setup 
    import logging
    reload(logging)
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG, datefmt='%I:%M:%S')
.. code:: python

    from flux.hooks import MySqlHook
    mysql_hook = MySqlHook(mysql_dbid='local_mysql')
    sql = """
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'flux'
    """
    mysql_hook.get_records(sql)



.. parsed-literal::

    (('flux', 'dag'),
     ('flux', 'dag_pickle'),
     ('flux', 'dag_picle'),
     ('flux', 'db_connection'),
     ('flux', 'deleteme'),
     ('flux', 'job'),
     ('flux', 'log'),
     ('flux', 'task'),
     ('flux', 'task_instance'),
     ('flux', 'tmp'),
     ('flux', 'user'))



Operators
---------

Operators allow you to perform a type of interaction with subsystems.
There are 3 main families of operator \* **Remote execution:** run a
Hive statement, run a map-reduce job, run a bash script, ... \*
**Sensors:** Wait for a Hive partition, wait for MySQL statement to
return a row count > 0, wait for a file to land in HDFS \* **Data
transfers:** Move data from Hive to MySQL, from a file into a Hive
table, from Oracle to Hive, ...

An operator instance is a task, and it represents a node in the DAG
(directed acyclic graph). A task defines a start\_date, end\_date (None
for open ended) and a schedule\_interval (say daily or hourly). Actual
task runs for a specific schedule time are what we refer to as task
instances.

Bellow we run a simple remote MySQL statement, over a date range. The
task.run() method will instanciate many task runs for the schedule
specified, and run them, storing the state in the Flux database. If you
were to re-run this, it would say it already succeded.

.. code:: python

    from flux.operators import MySqlOperator
    from datetime import datetime, timedelta
    
    sql = """
    INSERT INTO tmp
    SELECT 1;
    """
    mysql_op = MySqlOperator(task_id='test_task3', sql=sql, mysql_dbid='local_mysql', owner='max')
    mysql_op.run(start_date=datetime(2014, 9, 15), end_date=datetime(2014, 9, 17))

.. parsed-literal::

    INFO:
    --------------------------------------------------------------------------------
    New run starting @2014-10-08T14:51:23.362992
    --------------------------------------------------------------------------------
    INFO:Executing <Task(MySqlOperator): test_task3> for 2014-09-15 00:00:00
    INFO:Executing: 
    INSERT INTO tmp
    SELECT 1;
    INFO:
    --------------------------------------------------------------------------------
    New run starting @2014-10-08T14:51:23.608129
    --------------------------------------------------------------------------------
    INFO:Executing <Task(MySqlOperator): test_task3> for 2014-09-16 00:00:00
    INFO:Executing: 
    INSERT INTO tmp
    SELECT 1;
    INFO:
    --------------------------------------------------------------------------------
    New run starting @2014-10-08T14:51:23.776990
    --------------------------------------------------------------------------------
    INFO:Executing <Task(MySqlOperator): test_task3> for 2014-09-17 00:00:00
    INFO:Executing: 
    INSERT INTO tmp
    SELECT 1;


Creating a DAG
--------------

A DAG is simply a collection of tasks, with relationship between them,
and their associated task instance run states.

.. code:: python

    from flux.operators import MySqlOperator
    from flux import DAG
    from datetime import datetime
    
    # Setting some default operator parameters
    default_args = {
        'owner': 'max',
        'mysql_dbid': 'local_mysql',
    }
    
    # Initializing a directed acyclic graph
    dag = DAG(dag_id='test_dag')
    
    # MySQL Operator 
    sql = "TRUNCATE TABLE tmp;"
    mysql_fisrt = MySqlOperator(task_id='mysql_fisrt', sql=sql, **default_args)
    dag.add_task(mysql_fisrt)
    
    sql = """
    INSERT INTO tmp
    SELECT 1;
    """
    mysql_second = MySqlOperator(task_id='mysql_second', sql=sql, **default_args)
    dag.add_task(mysql_second)
    mysql_second.set_upstream(mysql_fisrt)
     
    dag.tree_view()
    dag.run(start_date=datetime(2014, 9, 1), end_date=datetime(2014, 9, 1))


.. parsed-literal::

    <Task(MySqlOperator): mysql_second>
        <Task(MySqlOperator): mysql_fisrt>


.. parsed-literal::

    INFO:Adding to queue: ./flux run test_dag mysql_fisrt 2014-09-01T00:00:00  --pickle 10  
    INFO:Adding to queue: ./flux run test_dag mysql_second 2014-09-01T00:00:00  --pickle 10  
    INFO:Run summary:


Templating with Jinja
---------------------

Jinja is a powerful templating engine in Python. It allows to nicely
integrate code logic, variables and call methods whithin your commands.

`Jinja2 documentation <http://jinja.pocoo.org/docs/dev/intro/>`_

By default all templated fields in operators get access to these
objects: \* **task\_instance** object with execution\_date \*
**macros**, a growing collection of useful methods \* **params**, a
flexible reference which you pass as you construct a task. You typically
would pass it a dictionary of constants, but you are free to pass an
entier module or any object here. Params is the Trojan horse from which
you pass parameters from your DAG code to your template. \* **dag**, a
reference to the current DAG object \* **task**, a reference to the
current task object

.. code:: python

    # Intergrate arbitrary macros in your code, grow the macro module
    sql = """
    INSERT INTO tmp
    SELECT {{ macros.random() * 100 }} 
    FROM t 
    WHERE ds='{{ macros.hive.latest_partition_for_table(some_other_table) }}';
    """
    
    # References to constants, execution_date
    sql = """
    INSERT OVWERWRITE TABLE {{ params["destination_table"] }} 
        PARTITON (ds='{{ task_instance.execution_date }}')
    SELECT field 
    FROM {{ params["source_table"] }}
    WHERE ds='{{ macros.latest_partition_for_table(some_other_table) }}';
    """
    
    # Code logic
    sql = """
    INSERT OVWERWRITE TABLE the_table
        PARTITON (ds='{{ task_instance.execution_date }}')
    {% if (mactros.datetime.now() - task_instance.execution_date).days > 90 %}
        SELECT * FROM anonymized_table;
    {% else %}
        SELECT * FROM non_anonymized_table;
    {% endif %}
    """
Command Line
------------

Perform any surgery you need from the command line. Fix false positive,
false negative, rerun subsection DAGs.

.. code:: python

    %%bash
    # Printing the --help for the main and subcommands
    cd /home/mistercrunch/Flux
    ./flux --help
    echo ============================================================================
    ./flux backfill -h
    echo ============================================================================
    ./flux clear -h
    echo ============================================================================
    ./flux run -h
    echo ============================================================================
    ./flux webserver -h
    echo ============================================================================
    ./flux master -h

.. parsed-literal::

    usage: flux_bin.py [-h] {backfill,clear,run,webserver,master} ...
    
    positional arguments:
      {backfill,clear,run,webserver,master}
                            sub-command help
        backfill            Run subsections of a DAG for a specified date range
        clear               Clear a set of task instance, as if they never ran
        run                 Run a single task instance
        webserver           Start a Flux webserver instance
        master              Start a master scheduler instance
    
    optional arguments:
      -h, --help            show this help message and exit
    ============================================================================
    usage: flux_bin.py backfill [-h] [-t TASK_REGEX] [-s START_DATE] [-e END_DATE] [-m] [-sd SUBDIR] dag_id
    
    positional arguments:
      dag_id                The id of the dag to run
    
    optional arguments:
      -h, --help            show this help message and exit
      -t TASK_REGEX, --task_regex TASK_REGEX
                            The regex to filter specific task_ids to backfill (optional)
      -s START_DATE, --start_date START_DATE
                            Overide start_date YYYY-MM-DD
      -e END_DATE, --end_date END_DATE
                            Overide end_date YYYY-MM-DD
      -m, --mark_success    Mark jobs as succeeded without running them
      -sd SUBDIR, --subdir SUBDIR
                            File location or directory from which to look for the dag
    ============================================================================
    usage: flux_bin.py clear [-h] [-t TASK_REGEX] [-s START_DATE] [-e END_DATE] [-sd SUBDIR] dag_id
    
    positional arguments:
      dag_id                The id of the dag to run
    
    optional arguments:
      -h, --help            show this help message and exit
      -t TASK_REGEX, --task_regex TASK_REGEX
                            The regex to filter specific task_ids to clear (optional)
      -s START_DATE, --start_date START_DATE
                            Overide start_date YYYY-MM-DD
      -e END_DATE, --end_date END_DATE
                            Overide end_date YYYY-MM-DD
      -sd SUBDIR, --subdir SUBDIR
                            File location or directory from which to look for the dag
    ============================================================================
    usage: flux_bin.py run [-h] [-sd SUBDIR] [-m] [-f] [-i] [-p PICKLE] dag_id task_id execution_date
    
    positional arguments:
      dag_id                The id of the dag to run
      task_id               The task_id to run
      execution_date        The execution date to run
    
    optional arguments:
      -h, --help            show this help message and exit
      -sd SUBDIR, --subdir SUBDIR
                            File location or directory from which to look for the dag
      -m, --mark_success    Mark jobs as succeeded without running them
      -f, --force           Force a run regardless or previous success
      -i, --ignore_dependencies
                            Ignore upstream and depends_on_past dependencies
      -p PICKLE, --pickle PICKLE
                            Serialized pickle object of the entire dag (used internally)
    ============================================================================
    usage: flux_bin.py webserver [-h] [-p PORT]
    
    optional arguments:
      -h, --help            show this help message and exit
      -p PORT, --port PORT  Set the port on which to run the web server
    ============================================================================
    usage: flux_bin.py master [-h] [-d DAG_ID] [-sd SUBDIR]
    
    optional arguments:
      -h, --help            show this help message and exit
      -d DAG_ID, --dag_id DAG_ID
                            The id of the dag to run
      -sd SUBDIR, --subdir SUBDIR
                            File location or directory from which to look for the dag


.. code:: python

    %%bash
    # Example run command
    cd $FLUX_HOME
    ./flux run example_2 runme_1 2014-09-01 -sd /home/mistercrunch/Flux/dags/examples/example2.py
    
    # Printing the log
    cat /home/mistercrunch/Flux/logs/example_2/runme_1/2014-09-01T00:00:00

.. parsed-literal::

    Logging into: /home/mistercrunch/Flux/logs/example_2/runme_1/2014-09-01T00:00:00
    [2014-10-08 15:11:39,177] {models.py:44} INFO - Filling up the DagBag from /home/mistercrunch/Flux/dags/examples/example2.py
    [2014-10-08 15:11:39,177] {models.py:53} INFO - Importing /home/mistercrunch/Flux/dags/examples/example2.py
    [2014-10-08 15:11:39,272] {models.py:325} INFO - 
    --------------------------------------------------------------------------------
    New run starting @2014-10-08T15:11:39.272355
    --------------------------------------------------------------------------------
    [2014-10-08 15:11:39,338] {models.py:359} INFO - Executing <Task(BashOperator): runme_1> for 2014-09-01 00:00:00
    [2014-10-08 15:11:39,343] {bash_operator.py:22} INFO - Runnning command: echo "1: 2014-09-01 00:00:00"
    [2014-10-08 15:11:39,346] {bash_operator.py:28} INFO - Command STDOUT:
    1: 2014-09-01 00:00:00
    


Creating an Operator
--------------------

Deriving BaseOperator is easy. You should create all the operators your
environment needs as building blocks factories for your pipelines.

Here's the source for the MySqlOperator

.. code:: python

    from core.models import BaseOperator                                            
    from core.hooks import MySqlHook                                                
                                                                                    
    class MySqlOperator(BaseOperator):                                              
                                                                                    
        __mapper_args__ = {'polymorphic_identity': 'MySqlOperator'} # SqlAlchemy artifact                                                                           
        template_fields = ('sql',) # the jinja template will be applied to these fields                                                  
                                                                                    
        def __init__(self, sql, mysql_dbid, *args, **kwargs):                       
            super(MySqlOperator, self).__init__(*args, **kwargs)                    
                                                                                    
            self.hook = MySqlHook(mysql_dbid=mysql_dbid)                            
            self.sql = sql                                                          
                                                                                    
        def execute(self, execution_date):                                          
            print('Executing:' + self.sql)                                          
            self.hook.run(self.sql)
Executors
---------

Executors are an abrastraction on top of systems that can run Flux task
instances. The default LocalExecutor is a simple implementation of
Python's multiprocessing with a simple joinable queue.

Arbitrary executors can be derived from BaseExecutor. Expect a Celery,
Redis/Mesos and other executors to be created soon.

.. code:: python

    # Coming up
