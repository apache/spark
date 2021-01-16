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

============
Installation
============

PySpark is included in the official releases of Spark available in the `Apache Spark website <https://spark.apache.org/downloads.html>`_.
For Python users, PySpark also provides ``pip`` installation from PyPI. This is usually for local usage or as
a client to connect to a cluster instead of setting up a cluster itself.
 
This page includes instructions for installing PySpark by using pip, Conda, downloading manually,
and building from the source.


Python Version Supported
------------------------

Python 3.6 and above.


Using PyPI
----------

PySpark installation using `PyPI <https://pypi.org/project/pyspark/>`_ is as follows:

.. code-block:: bash

    pip install pyspark

If you want to install extra dependencies for a specific component, you can install it as below:

.. code-block:: bash

    pip install pyspark[sql]

For PySpark with/without a specific Hadoop version, you can install it by using ``PYSPARK_HADOOP_VERSION`` environment variables as below:

.. code-block:: bash

    PYSPARK_HADOOP_VERSION=2.7 pip install pyspark

The default distribution uses Hadoop 3.2 and Hive 2.3. If users specify different versions of Hadoop, the pip installation automatically
downloads a different version and use it in PySpark. Downloading it can take a while depending on
the network and the mirror chosen. ``PYSPARK_RELEASE_MIRROR`` can be set to manually choose the mirror for faster downloading.

.. code-block:: bash

    PYSPARK_RELEASE_MIRROR=http://mirror.apache-kr.org PYSPARK_HADOOP_VERSION=2.7 pip install

It is recommended to use ``-v`` option in ``pip`` to track the installation and download status.

.. code-block:: bash

    PYSPARK_HADOOP_VERSION=2.7 pip install pyspark -v

Supported values in ``PYSPARK_HADOOP_VERSION`` are:

- ``without``: Spark pre-built with user-provided Apache Hadoop
- ``2.7``: Spark pre-built for Apache Hadoop 2.7
- ``3.2``: Spark pre-built for Apache Hadoop 3.2 and later (default)

Note that this installation way of PySpark with/without a specific Hadoop version is experimental. It can change or be removed between minor releases.


Using Conda
-----------

Conda is an open-source package management and environment management system which is a part of
the `Anaconda <https://docs.continuum.io/anaconda/>`_ distribution. It is both cross-platform and
language agnostic. In practice, Conda can replace both `pip <https://pip.pypa.io/en/latest/>`_ and
`virtualenv <https://virtualenv.pypa.io/en/latest/>`_.

Create new virtual environment from your terminal as shown below:

.. code-block:: bash

    conda create -n pyspark_env

After the virtual environment is created, it should be visible under the list of Conda environments
which can be seen using the following command:

.. code-block:: bash

    conda env list

Now activate the newly created environment with the following command:

.. code-block:: bash

    conda activate pyspark_env

You can install pyspark by `Using PyPI <#using-pypi>`_ to install PySpark in the newly created
environment, for example as below. It will install PySpark under the new virtual environment
``pyspark_env`` created above.

.. code-block:: bash

    pip install pyspark

Alternatively, you can install PySpark from Conda itself as below:

.. code-block:: bash

    conda install pyspark

However, note that `PySpark at Conda <https://anaconda.org/conda-forge/pyspark>`_ is not necessarily
synced with PySpark release cycle because it is maintained by the community separately.


Manually Downloading
--------------------

PySpark is included in the distributions available at the `Apache Spark website <https://spark.apache.org/downloads.html>`_.
You can download a distribution you want from the site. After that, uncompress the tar file into the directory where you want
to install Spark, for example, as below:

.. code-block:: bash

    tar xzvf spark-3.0.0-bin-hadoop2.7.tgz

Ensure the ``SPARK_HOME`` environment variable points to the directory where the tar file has been extracted.
Update ``PYTHONPATH`` environment variable such that it can find the PySpark and Py4J under ``SPARK_HOME/python/lib``.
One example of doing this is shown below:

.. code-block:: bash

    cd spark-3.0.0-bin-hadoop2.7
    export SPARK_HOME=`pwd`
    export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH


Installing from Source
----------------------

To install PySpark from source, refer to |building_spark|_.


Dependencies
------------
============= ========================= ================
Package       Minimum supported version Note
============= ========================= ================
`pandas`      0.23.2                    Optional for SQL
`NumPy`       1.7                       Required for ML 
`pyarrow`     1.0.0                     Optional for SQL
`Py4J`        0.10.9                    Required
============= ========================= ================

Note that PySpark requires Java 8 or later with ``JAVA_HOME`` properly set.  
If using JDK 11, set ``-Dio.netty.tryReflectionSetAccessible=true`` for Arrow related features and refer
to |downloading|_.
