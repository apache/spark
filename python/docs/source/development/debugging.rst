..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

=================
Debugging PySpark
=================

PySpark uses Spark as an engine. PySpark uses `Py4J <https://www.py4j.org/>`_ to leverage Spark to submit and computes the jobs.

On the driver side, PySpark communicates with the driver on JVM by using `Py4J <https://www.py4j.org/>`_.
When :class:`pyspark.sql.SparkSession` or :class:`pyspark.SparkContext` is created and initialized, PySpark launches a JVM
to communicate.

On the executor side, Python workers execute and handle Python native functions or data. They are not launched if
a PySpark application does not require interaction between Python workers and JVMs. They are lazily launched only when
Python native functions or data have to be handled, for example, when you execute pandas UDFs or
PySpark RDD APIs.

This page focuses on debugging Python side of PySpark on both driver and executor sides instead of focusing on debugging
with JVM. Profiling and debugging JVM is described at `Useful Developer Tools <https://spark.apache.org/developer-tools.html>`_.

Note that,

* PySpark in driver side is just like a regular Python program if you are running locally. You can directly debug via using
    your IDE without the remote debug feature.

* There are many other ways of debugging PySpark applications. For example, you can remotely debug by
    using the open source `Remote Debugger <https://www.pydev.org/manual_adv_remote_debugger.html>`_ instead of using
    PyCharm Professional documented here. 


Remote Debugging (PyCharm Professional)
---------------------------------------

This section describes remote debugging within a single machine to demonstrate easily.
The ways of debugging PySpark on the executor side is different from doing in the driver. They will be demonstrated respectively.
In order to debug PySpark applications on other machines, please refer to the full instructions that are specific
to PyCharm, documented `here <https://www.jetbrains.com/help/pycharm/remote-debugging-with-product.html>`_.

Firstly, choose **Edit Configuration...** from the **Run** menu. It opens the **Run/Debug Configurations dialog**.
You have to click ``+`` configuration on the toolbar, and from the list of available configurations, select **Python Debug Server**.
Enter the name of this new configuration, for example, ``MyRemoteDebugger`` and also specify the port number, for example ``12345``.

.. image:: ../../../../docs/img/pyspark-remote-debug1.png
    :alt: PyCharm remote debugger setting

| After that, you should install the corresponding version of the ``pydevd-pycahrm`` package. In the previous dialog, it shows the command to install.

.. code-block:: text

    pip install pydevd-pycharm~=<version of PyCharm on the local machine>


Debugging Driver Side
~~~~~~~~~~~~~~~~~~~~~

Add, on the top of your PySpark application script, the codes with ``pydevd_pycharm.settrace`` to connect to the
debugging server in PyCharm. Suppose the file name is ``app.py``:

.. code-block:: python

    #======================Copy and paste from the previous dialog===========================
    import pydevd_pycharm
    pydevd_pycharm.settrace('localhost', port=12345, stdoutToServer=True, stderrToServer=True)
    #========================================================================================
    # Your PySpark application codes:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark.range(10).show()

Now you're ready to remotely debug. Start debugging with your ``MyRemoteDebugger``.

.. image:: ../../../../docs/img/pyspark-remote-debug2.png
    :alt: PyCharm run remote debugger

| After that, run your application.

.. code-block:: bash

    spark-submit app.py


Debugging Executor Side
~~~~~~~~~~~~~~~~~~~~~~~

In your current working directory, prepare a Python file as below:

.. code-block:: bash

    echo "from pyspark import daemon, worker
    def remote_debug_wrapped(*args, **kwargs):
        #======================Copy and paste from the previous dialog===========================
        import pydevd_pycharm
        pydevd_pycharm.settrace('localhost', port=12345, stdoutToServer=True, stderrToServer=True)
        #========================================================================================
        worker.main(*args, **kwargs)
    daemon.worker_main = remote_debug_wrapped
    if __name__ == '__main__':
        daemon.manager()" > remote_debug.py

You will use this file as the Python worker in your PySpark applications by using the ``spark.python.daemon.module`` configuration.
Run the ``pyspark`` shell with the configuration below:

.. code-block:: bash

    pyspark --conf spark.python.daemon.module=remote_debug

Now you're ready to remotely debug. Start debugging with your ``MyRemoteDebugger``.

.. image:: ../../../../docs/img/pyspark-remote-debug2.png
    :alt: PyCharm run remote debugger

| After that, run a job that creates Python workers, for example, as below:

.. code-block:: python

    spark.range(10).repartition(1).rdd.map(lambda x: x).collect()



Checking Memory and CPU Usage
-----------------------------


Driver Side
~~~~~~~~~~~

To monitor the driver side in PySpark, there are many tools you can casually use to monitor regular Python processes such as `memory_profiler <https://github.com/pythonprofilers/memory_profiler>`_ which provides an easy way of profiling, for example:

.. code-block:: bash

    echo "from memory_profiler import profile
    from pyspark.sql import SparkSession
    @profile
    def my_func():
        session = SparkSession.builder.getOrCreate()
        df = session.range(10000)
        return df.collect()
    if __name__ == '__main__':
        my_func()" > profile_memory.py

.. code-block:: bash

    python -m memory_profiler profile_memory.py

.. code-block:: text

    Filename: profile_memory.py

    Line #    Mem usage    Increment   Line Contents
    ================================================
         3     50.5 MiB     50.5 MiB   @profile
         4                             def my_func():
         5     51.2 MiB      0.7 MiB       session = SparkSession.builder.getOrCreate()
         6     51.2 MiB      0.0 MiB       df = session.range(10000)
         7     54.1 MiB      2.9 MiB       return df.collect()


Executor Side
~~~~~~~~~~~~~

Python workers are typically monitored via ``top`` and ``ps`` commands because Python workers create multiple Python processes
workers. As an example, you can ``ps`` as below:

.. code-block:: bash

    ps -fe | grep pyspark.daemon

.. code-block:: text

    000 12345     1   0  0:00PM ttys000    0:00.00 /.../python -m pyspark.daemon
    000 12345     1   0  0:00PM ttys000    0:00.00 /.../python -m pyspark.daemon
    000 12345     1   0  0:00PM ttys000    0:00.00 /.../python -m pyspark.daemon
    000 12345     1   0  0:00PM ttys000    0:00.00 /.../python -m pyspark.daemon
    ...


Python Profilers
----------------

Driver Side
~~~~~~~~~~~~~

`Python Profilers <https://docs.python.org/3/library/profile.html>`_ is a useful built-in feature in Python itself.
PySpark on driver side is just a regular Python process so you can use it as you do for regular Python programs.

.. code-block:: bash

    echo "from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark.range(10).show()" > app.py

.. code-block:: bash

    python -m cProfile app.py

.. code-block:: text

    ...
         129215 function calls (125446 primitive calls) in 5.926 seconds

       Ordered by: standard name

       ncalls  tottime  percall  cumtime  percall filename:lineno(function)
     1198/405    0.001    0.000    0.083    0.000 <frozen importlib._bootstrap>:1009(_handle_fromlist)
          561    0.001    0.000    0.001    0.000 <frozen importlib._bootstrap>:103(release)
          276    0.000    0.000    0.000    0.000 <frozen importlib._bootstrap>:143(__init__)
          276    0.000    0.000    0.002    0.000 <frozen importlib._bootstrap>:147(__enter__)
    ...



Executor Side
~~~~~~~~~~~~~

PySpark provides remote `Python Profilers <https://docs.python.org/3/library/profile.html>`_ for executor side, which can be
enabled by setting ``spark.python.profile`` configuration to ``true``.

.. code-block:: bash

    pyspark --conf spark.python.profile=true


.. code-block:: python

    >>> rdd = sc.parallelize(range(100)).map(str)
    >>> rdd.count()
    100
    >>> sc.show_profiles()
    ============================================================
    Profile of RDD<id=1>
    ============================================================
             728 function calls (692 primitive calls) in 0.004 seconds

       Ordered by: internal time, cumulative time

       ncalls  tottime  percall  cumtime  percall filename:lineno(function)
           12    0.001    0.000    0.001    0.000 serializers.py:210(load_stream)
           12    0.000    0.000    0.000    0.000 {built-in method _pickle.dumps}
           12    0.000    0.000    0.001    0.000 serializers.py:252(dump_stream)
           12    0.000    0.000    0.001    0.000 context.py:506(f)
    ...

This feature is supported only with RDD APIs.
