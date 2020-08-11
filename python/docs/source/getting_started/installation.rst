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
Installation
===============

Using Conda 
~~~~~~~~~~~~~~~~~~
PySpark installation using `Conda <https://anaconda.org/conda-forge/pyspark>`_ can be performed using the below command::

    conda install -c conda-forge pyspark
   
Using PyPI
~~~~~~~~~~
PySpark installation using `PyPI <https://pypi.org/project/pyspark/>`_::

    pip install pyspark

Official release channel
~~~~~~~~~~~~~~~~~~~~~~~~

Different flavor of PySpark is available in `the official release channel <https://spark.apache.org/downloads.html>`__.
Any suitable version can be downloaded and extracted as below::

    tar xzvf spark-3.0.0-bin-hadoop2.7.tgz

An important step is to ensure ``SPARK_HOME`` environment variable points to the directory where the code has been extracted. The next step is to properly define ``PYTHONPATH`` such that it can find the PySpark and Py4J under ``$SPARK_HOME/python/lib``::

    cd spark-3.0.0-bin-hadoop2.7

    export SPARK_HOME=`pwd`

    export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH

Installing from source
~~~~~~~~~~~~~~~~~~~~~~

To install PySpark from source, refer `Building Spark <https://spark.apache.org/docs/latest/building-spark.html>`__.

* Steps for defining ``PYTHONPATH`` is same as described in `Official release channel` section above. 


Dependencies
------------
* Using PySpark requires the Spark JARs.
* At its core PySpark depends on Py4J, but some additional sub-packages have their own extra requirements for some features (including NumPy, pandas, and PyArrow).