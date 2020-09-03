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

The official release channel is to download it from `the Apache Spark website <https://spark.apache.org/downloads.html>`_.
Alternatively, you can also install it via pip from PyPI.  PyPI installation is usually to use
standalone locally or as a client to connect to a cluster instead of setting a cluster up.  
 
This page includes the instructions for installing PySpark by using pip, Conda, downloading manually, and building it from the source.

Python Version Supported
------------------------

Python 3.6 and above.

Using PyPI
----------

PySpark installation using `PyPI <https://pypi.org/project/pyspark/>`_

.. code-block:: bash

    pip install pyspark
	
Using Conda  
-----------

Conda is an open-source package management and environment management system which is a part of `Anaconda <https://docs.continuum.io/anaconda/>`_ distribution. It is both cross-platform and language agnostic.
  
Conda can be used to create a virtual environment from terminal as shown below:

.. code-block:: bash

	conda create -n pyspark_env 

After the virtual environment is created, it should be visible under the list of conda environments which can be seen using the following command:

.. code-block:: bash

	conda env list

The newly created environment can be accessed using the following command:

.. code-block:: bash

	conda activate pyspark_env

In lower Conda version, the following command might be used:

.. code-block:: bash

	source activate pyspark_env

PySpark installation using ``pip`` under Conda environment is official. 

PySpark can be installed in this newly created environment using PyPI as shown before:

.. code-block:: bash

	pip install pyspark

`PySpark at Conda <https://anaconda.org/conda-forge/pyspark>`_ is not the official release.

Official Release Channel
------------------------

Different flavor of PySpark is available in `the official release channel <https://spark.apache.org/downloads.html>`_.
Any suitable version can be downloaded and extracted as below:

.. code-block:: bash

    tar xzvf spark-3.0.0-bin-hadoop2.7.tgz

An important step is to ensure ``SPARK_HOME`` environment variable points to the directory where the code has been extracted. 
The next step is to properly define ``PYTHONPATH`` such that it can find the PySpark and 
Py4J under ``$SPARK_HOME/python/lib``, one example of doing this is shown below:

.. code-block:: bash

    cd spark-3.0.0-bin-hadoop2.7
    export SPARK_HOME=`pwd`
    export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH

Installing from Source
----------------------

To install PySpark from source, refer `Building Spark <https://spark.apache.org/docs/latest/building-spark.html>`_.

Steps for defining ``PYTHONPATH`` is same as described in `Official Release Channel <#official-release-channel>`_. 

Dependencies
------------
============= ========================= ====================
Package       Minimum supported version Note
============= ========================= ====================
`pandas`      0.23.2                    Optional
`NumPy`       1.7                       Optional
`pyarrow`     0.15.1                    Optional
`Py4J`        0.10.9                    Required
============= ========================= ====================

**Note**: A prerequisite for PySpark installation is the availability of ``JAVA 8 or 11`` and ``JAVA_HOME`` properly set.