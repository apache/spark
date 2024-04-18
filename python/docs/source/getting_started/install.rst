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


Python Versions Supported
-------------------------

Python 3.8 and above.


Using PyPI
----------

PySpark installation using `PyPI (pyspark) <https://pypi.org/project/pyspark/>`_ is as follows:

.. code-block:: bash

    pip install pyspark

If you want to install extra dependencies for a specific component, you can install it as below:

.. code-block:: bash

    # Spark SQL
    pip install pyspark[sql]
    # pandas API on Spark
    pip install pyspark[pandas_on_spark] plotly  # to plot your data, you can install plotly together.
    # Spark Connect
    pip install pyspark[connect]


See :ref:`optional-dependencies` for more detail about extra dependencies.

For PySpark with/without a specific Hadoop version, you can install it by using ``PYSPARK_HADOOP_VERSION`` environment variables as below:

.. code-block:: bash

    PYSPARK_HADOOP_VERSION=3 pip install pyspark

The default distribution uses Hadoop 3.3 and Hive 2.3. If users specify different versions of Hadoop, the pip installation automatically
downloads a different version and uses it in PySpark. Downloading it can take a while depending on
the network and the mirror chosen. ``PYSPARK_RELEASE_MIRROR`` can be set to manually choose the mirror for faster downloading.

.. code-block:: bash

    PYSPARK_RELEASE_MIRROR=http://mirror.apache-kr.org PYSPARK_HADOOP_VERSION=3 pip install

It is recommended to use ``-v`` option in ``pip`` to track the installation and download status.

.. code-block:: bash

    PYSPARK_HADOOP_VERSION=3 pip install pyspark -v

Supported values in ``PYSPARK_HADOOP_VERSION`` are:

- ``without``: Spark pre-built with user-provided Apache Hadoop
- ``3``: Spark pre-built for Apache Hadoop 3.3 and later (default)

Note that this installation of PySpark with/without a specific Hadoop version is experimental. It can change or be removed between minor releases.


Python Spark Connect Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Python Spark Connect client is a pure Python library that does not rely on any non-Python dependencies such as jars and JRE in your environment.
To install the Python Spark Connect client via `PyPI (pyspark-connect) <https://pypi.org/project/pyspark-connect/>`_, execute the following command:

.. code-block:: bash

    pip install pyspark-connect

See also `Quickstart: Spark Connect <quickstart_connect.html>`_ for how to use it.


Using Conda
-----------

Conda is an open-source package management and environment management system (developed by
`Anaconda <https://www.anaconda.com/>`_), which is best installed through
`Miniconda <https://docs.conda.io/en/latest/miniconda.html>`_ or `Miniforge <https://github.com/conda-forge/miniforge/>`_.
The tool is both cross-platform and language agnostic, and in practice, conda can replace both
`pip <https://pip.pypa.io/en/latest/>`_ and `virtualenv <https://virtualenv.pypa.io/en/latest/>`_.

Conda uses so-called channels to distribute packages, and together with the default channels by
Anaconda itself, the most important channel is `conda-forge <https://conda-forge.org/>`_, which
is the community-driven packaging effort that is the most extensive & the most current (and also
serves as the upstream for the Anaconda channels in most cases).

To create a new conda environment from your terminal and activate it, proceed as shown below:

.. code-block:: bash

    conda create -n pyspark_env
    conda activate pyspark_env

After activating the environment, use the following command to install pyspark,
a python version of your choice, as well as other packages you want to use in
the same session as pyspark (you can install in several steps too).

.. code-block:: bash

    conda install -c conda-forge pyspark  # can also add "python=3.8 some_package [etc.]" here

Note that `PySpark for conda <https://anaconda.org/conda-forge/pyspark>`_ is maintained
separately by the community; while new versions generally get packaged quickly, the
availability through conda(-forge) is not directly in sync with the PySpark release cycle.

While using pip in a conda environment is technically feasible (with the same command as
`above <#using-pypi>`_), this approach is `discouraged <https://www.anaconda.com/blog/using-pip-in-a-conda-environment/>`_,
because pip does not interoperate with conda.

For a short summary about useful conda commands, see their
`cheat sheet <https://docs.conda.io/projects/conda/en/latest/user-guide/cheatsheet.html>`_.


Manually Downloading
--------------------

PySpark is included in the distributions available at the `Apache Spark website <https://spark.apache.org/downloads.html>`_.
You can download a distribution you want from the site. After that, uncompress the tar file into the directory where you want
to install Spark, for example, as below:

.. parsed-literal::

    tar xzvf spark-\ |release|\-bin-hadoop3.tgz

Ensure the ``SPARK_HOME`` environment variable points to the directory where the tar file has been extracted.
Update ``PYTHONPATH`` environment variable such that it can find the PySpark and Py4J under ``SPARK_HOME/python/lib``.
One example of doing this is shown below:

.. parsed-literal::

    cd spark-\ |release|\-bin-hadoop3
    export SPARK_HOME=`pwd`
    export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH


Installing from Source
----------------------

To install PySpark from source, refer to |building_spark|_.


Dependencies
------------

Required dependencies
~~~~~~~~~~~~~~~~~~~~~

PySpark requires the following dependencies.

========================== ========================= =============================
Package                    Supported version         Note
========================== ========================= =============================
`py4j`                     >=0.10.9.7                Required to interact with JVM
========================== ========================= =============================

Additional libraries that enhance functionality but are not included in the installation packages:

- **memory-profiler**: Used for PySpark UDF memory profiling, ``spark.profile.show(...)`` and ``spark.sql.pyspark.udf.profiler``.

Note that PySpark requires Java 17 or later with ``JAVA_HOME`` properly set and refer to |downloading|_.


.. _optional-dependencies:

Optional dependencies
~~~~~~~~~~~~~~~~~~~~~

PySpark has several optional dependencies that enhance its functionality for specific modules.
These dependencies are only required for certain features and are not necessary for the basic functionality of PySpark.
If these optional dependencies are not installed, PySpark will function correctly for basic operations but will raise an ``ImportError``
when you try to use features that require these dependencies.

Spark Connect
^^^^^^^^^^^^^

Installable with ``pip install "pyspark[connect]"``.

========================== ================= ==========================
Package                    Supported version Note
========================== ================= ==========================
`pandas`                   >=1.4.4           Required for Spark Connect
`pyarrow`                  >=10.0.0          Required for Spark Connect
`grpcio`                   >=1.62.0          Required for Spark Connect
`grpcio-status`            >=1.62.0          Required for Spark Connect
`googleapis-common-protos` >=1.56.4          Required for Spark Connect
========================== ================= ==========================

Spark SQL
^^^^^^^^^

Installable with ``pip install "pyspark[sql]"``.

========= ================= ======================
Package   Supported version Note
========= ================= ======================
`pandas`  >=1.4.4           Required for Spark SQL
`pyarrow` >=10.0.0          Required for Spark SQL
========= ================= ======================


Pandas API on Spark
^^^^^^^^^^^^^^^^^^^

Installable with ``pip install "pyspark[pandas_on_spark]"``.

========= ================= ================================
Package   Supported version Note
========= ================= ================================
`pandas`  >=1.4.4           Required for Pandas API on Spark
`pyarrow` >=10.0.0          Required for Pandas API on Spark
========= ================= ================================

Additional libraries that enhance functionality but are not included in the installation packages:

- **mlflow**: Required for ``pyspark.pandas.mlflow``.
- **plotly**: Provide plotting for visualization. It is recommended using **plotly** over **matplotlib**.
- **matplotlib**: Provide plotting for visualization. The default is **plotly**.


MLLib DataFrame-based API
^^^^^^^^^^^^^^^^^^^^^^^^^

Installable with ``pip install "pyspark[ml]"``.

======= ================= ======================================
Package Supported version Note
======= ================= ======================================
`numpy` >=1.21            Required for MLLib DataFrame-based API
======= ================= ======================================

Additional libraries that enhance functionality but are not included in the installation packages:

- **scipy**: Required for SciPy integration.
- **scikit-learn**: Required for implementing machine learning algorithms.
- **torch**: Required for machine learning model training.
- **torchvision**: Required for supporting image and video processing.
- **torcheval**: Required for facilitating model evaluation metrics.
- **deepspeed**: Required for providing high-performance model training optimizations. Installable on non-Darwin systems.

MLlib
^^^^^

Installable with ``pip install "pyspark[mllib]"``.

======= ================= ==================
Package Supported version Note
======= ================= ==================
`numpy` >=1.21            Required for MLLib
======= ================= ==================
