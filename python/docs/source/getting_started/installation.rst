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

Using PyPI
~~~~~~~~~~
PySpark installation using `PyPI <https://pypi.org/project/pyspark/>`_::

    pip install pyspark
	
Using Miniconda  
~~~~~~~~~~~~~~~
Pyspark is available in Conda but it is not a part of an official release. Conda can be used to create a virtual environment from terminal as shown below::

	conda create -n ENV_NAME python==PYTHON_VERSION

After the virtual environment is created, it should be visible under the list of conda environments which can be seen using the following command::

	conda env list

The newly created environment can be accessed using the following command::

	source activate ENV_NAME

On Windows, the command is::

	conda activate ENV_NAME

PySpark can be installed in this newly created environment using PyPI as shown before::

	pip install pyspark

Official release channel
~~~~~~~~~~~~~~~~~~~~~~~~

Different flavor of PySpark is available in `the official release channel <https://spark.apache.org/downloads.html>`__.
Any suitable version can be downloaded and extracted as below::

    tar xzvf spark-3.0.0-bin-hadoop2.7.tgz

An important step is to ensure ``SPARK_HOME`` environment variable points to the directory where the code has been extracted. The next step is to properly define ``PYTHONPATH`` such that it can find the PySpark and Py4J under ``$SPARK_HOME/python/lib``, one example of doing this is shown below::

    cd spark-3.0.0-bin-hadoop2.7

    export SPARK_HOME=`pwd`

    export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH

Installing from source
~~~~~~~~~~~~~~~~~~~~~~

To install PySpark from source, refer `Building Spark <https://spark.apache.org/docs/latest/building-spark.html>`__.

* Steps for defining ``PYTHONPATH`` is same as described in `Official release channel` section above. 


Dependencies
------------
============= =========================
Package       Minimum supported version
============= =========================
`pandas`      0.23.2
`NumPy`       1.7
`pyarrow`     0.15.1
`Py4J`        0.10.9
============= =========================