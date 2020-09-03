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

PySpark uses Spark as an engine. In case that PySpark applications do not require the interaction
between Python workers and JVMs, Python workers are not launched. They are lazily launched only when
Python native functions or data have to be handled, for example, when you execute pandas UDFs or
PySpark RDD APIs.

This page describes how to debug such Python workers instead of focusing on debugging with JVM.
Profiling and debugging JVM is described at `Useful Developer Tools <https://spark.apache.org/developer-tools.html>`_.


Remote Debugging (PyCharm)
--------------------------

In order to debug the Python workers remotely, you should connect from the Python worker to the debug server in PyCharm.
In this section, it describes the remote debug within single machine to demonstrate easily.
In order to debug PySpark applications in other machines, please refer to the full instructions that are specific
to PyCharm is documented at `here <https://www.jetbrains.com/help/pycharm/remote-debugging-with-product.html#remote-debug-config>`_. 

Firstly, choose **Run | Edit Configuration...** in the main manu, and it opens the Run/debug configurations dialog.
You have to click ``+`` configuration on the toolbar, and from the list of available configurations, select **Python Debug Server**.
Enter the name of this run/debug configuration, for example, ``MyRemoteDebugger`` and also specify the port number, for example ``12345``.

.. image:: ../../../../docs/img/pyspark-remote-debug1.png
    :alt: PyCharm remote debugger setting

After that, you should install the corresponding version of ``pydevd-pycahrm`` package. In the previous dialog, it shows the command
to install.

.. code-block:: text

    pip install pydevd-pycharm~=<version of PyCharm on the local machine>

In your current working directly, prepare a Python file as below:

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

You will use this file as the Python workers in your PySpark applications by using ``spark.python.daemon.module`` configuration.
Run ``pyspark`` shell with the configuration below:

.. code-block:: bash

    pyspark --conf spark.python.daemon.module=remote_debug

Now you're ready to remote debug. Start debug with your ``MyRemoteDebugger``.

.. image:: ../../../../docs/img/pyspark-remote-debug2.png
    :alt: PyCharm run remote debugger

After that, run a job that creates a Python workers, for example, as below:

.. code-block:: python

    spark.range(10).repartition(1).rdd.map(lambda x: x).collect()


Checking Memory and CPU Usage
-----------------------------

Python workers are typically monitored via ``top`` and ``ps`` commands because Python workers launch multiple Python
workers are created as processes. As an example, you can ``ps`` as below:

.. code-block:: bash

    ps -fe | grep pyspark.daemon

.. code-block:: text

    000 12345     1   0  0:00PM ttys000    0:00.00 /.../python -m pyspark.daemon
    000 12345     1   0  0:00PM ttys000    0:00.00 /.../python -m pyspark.daemon
    000 12345     1   0  0:00PM ttys000    0:00.00 /.../python -m pyspark.daemon
    000 12345     1   0  0:00PM ttys000    0:00.00 /.../python -m pyspark.daemon
    ...


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


PySpark Profiler
----------------

PySpark provides remote `Python Profilers <https://docs.python.org/3/library/profile.html>`_, which can be
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
           12    0.000    0.000    0.004    0.000 worker.py:589(process)
        48/12    0.000    0.000    0.002    0.000 rdd.py:2610(pipeline_func)
           12    0.000    0.000    0.000    0.000 rdd.py:363(func)
          112    0.000    0.000    0.000    0.000 util.py:66(wrapper)
           36    0.000    0.000    0.000    0.000 rdd.py:391(func)
           24    0.000    0.000    0.000    0.000 context.py:503(getStart)
           12    0.000    0.000    0.000    0.000 serializers.py:558(read_int)
           12    0.000    0.000    0.000    0.000 serializers.py:213(_load_stream_without_unbatching)
           12    0.000    0.000    0.000    0.000 util.py:61(fail_on_stopiteration)
           12    0.000    0.000    0.000    0.000 serializers.py:565(write_int)
           12    0.000    0.000    0.000    0.000 serializers.py:148(_read_with_length)
          112    0.000    0.000    0.000    0.000 rdd.py:1113(<genexpr>)
           12    0.000    0.000    0.000    0.000 {built-in method builtins.hasattr}
           12    0.000    0.000    0.000    0.000 serializers.py:132(load_stream)
           12    0.000    0.000    0.000    0.000 rdd.py:1113(<lambda>)
           12    0.000    0.000    0.000    0.000 {built-in method from_iterable}
           12    0.000    0.000    0.000    0.000 {built-in method _operator.add}
           24    0.000    0.000    0.000    0.000 {built-in method builtins.sum}
           12    0.000    0.000    0.001    0.000 serializers.py:423(dumps)
           12    0.000    0.000    0.000    0.000 {built-in method _struct.unpack}
    ...

This feature is supported only with RDD APIs.