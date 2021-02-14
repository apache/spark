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
