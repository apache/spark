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

===============
Testing PySpark
===============

In order to run PySpark tests, you should build Spark itself first via Maven or SBT. For example,

.. code-block:: bash

    build/mvn -DskipTests clean package

After that, the PySpark test cases can be run via using ``python/run-tests``. For example,

.. code-block:: bash

    python/run-tests --python-executable=python3

Note that you may set ``OBJC_DISABLE_INITIALIZE_FORK_SAFETY`` environment variable to ``YES`` if you are running tests on Mac OS.

Please see the guidance on how to `build Spark <https://github.com/apache/spark#building-spark>`_,
`run tests for a module, or individual tests <https://spark.apache.org/developer-tools.html>`_.


Running Individual PySpark Tests
--------------------------------

You can run a specific test via using ``python/run-tests``, for example, as below:

.. code-block:: bash

    python/run-tests --testnames pyspark.sql.tests.test_arrow

Please refer to `Testing PySpark <https://spark.apache.org/developer-tools.html>`_ for more details.


Running tests using GitHub Actions
----------------------------------

You can run the full PySpark tests by using GitHub Actions in your own forked GitHub
repository with a few clicks. Please refer to
`Running tests in your forked repository using GitHub Actions <https://spark.apache.org/developer-tools.html>`_ for more details.


===================================
Testing Spark Connect Python Client
===================================

**Spark Connect is a strictly experimental feature and under heavy development.
All APIs should be considered volatile and should not be used in production.**

This module contains the implementation of Spark Connect which is a logical plan
facade for the implementation in Spark. Spark Connect is directly integrated into the build
of Spark. To enable it, you only need to activate the driver plugin for Spark Connect.


Build
-----

.. code-block:: bash

    ./build/mvn -Phive clean package

or

.. code-block:: bash

    ./build/sbt -Phive clean package


Run Spark Shell
---------------

To run Spark Connect you locally built:

.. code-block:: bash

    # Scala shell
    ./bin/spark-shell \
    --jars `ls connector/connect/target/**/spark-connect*SNAPSHOT.jar | paste -sd ',' -` \
    --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin

.. code-block:: bash

    # PySpark shell
    ./bin/pyspark \
    --jars `ls connector/connect/target/**/spark-connect*SNAPSHOT.jar | paste -sd ',' -` \
    --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin

To use the release version of Spark Connect:

.. code-block:: bash

    ./bin/spark-shell \
    --packages org.apache.spark:spark-connect_2.12:3.4.0 \
    --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin


Run Tests
---------

.. code-block:: bash

    ./python/run-tests --testnames 'pyspark.sql.tests.connect.test_connect_basic'


Generate proto generated files for the Python client
----------------------------------------------------

1. Install `buf version 1.8.0`: https://docs.buf.build/installation
2. Run `pip install grpcio==1.48.1 protobuf==4.21.6 mypy-protobuf==3.3.0`
3. Run `./connector/connect/dev/generate_protos.sh`
4. Optional Check `./dev/check-codegen-python.py`
