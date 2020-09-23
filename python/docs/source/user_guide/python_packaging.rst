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


################
Python packaging
################

When you want to run your PySpark application on a cluster (like YARN, Kubernetes, Mesos, ..) you need to make sure that the your code
and all used libraries are available on the executors.

As an example let's say you may want to run the `Pandas UDF's examples <arrow_pandas.rst#series-to-scalar>`_.
As it uses pyarrow as an underlying implementation we need to make sure to have pyarrow installed on each executor on the cluster. Otherwise you may get errors such as 
``ModuleNotFoundError: No module named 'pyarrow'``.

Here is the script ``main.py`` from the previous example that will be executed on the cluster:

.. code-block:: python

  import pandas as pd
  from pyspark.sql.functions import pandas_udf, PandasUDFType
  from pyspark.sql import SparkSession

  def main(spark):
    df = spark.createDataFrame(
      [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
      ("id", "v"))

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(v: pd.Series):
      return v.mean()

    print(df.groupby("id").agg(mean_udf(df['v'])).collect())


  if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    main(spark)


There are multiple ways to ship the dependencies to the cluster:

- Using py-files
- Using a zipped virtual environment
- Using PEX
- Using Docker


**************
Using py-files
**************

PySpark allows to upload python files to the executors by setting the configuration setting ``spark.submit.pyFiles`` or by directly calling `addPyFile
<../reference/api/pyspark.SparkContext.addPyFile.rst>`_ on the SparkContext.

This is an easy way to ship additional custom Python code to the cluster. You can just add individual files or zip whole packages and upload them. 
Using `addPyFile <../reference/api/pyspark.SparkContext.addPyFile.rst>`_ allows to upload code even after having started your job.

It doesn't allow to add packages built as `Wheels <https://www.python.org/dev/peps/pep-0427/>`_ and therefore doesn't allowing to include dependencies with native code.


**********************************
Using a zipped virtual environment
**********************************

The idea of zipped environments is to zip your whole `virtual environment <https://docs.python.org/3/tutorial/venv.html>`_, 
ship it to the cluster, unzip it remotly and target the Python interpreter from inside this zipped environment.

Zip your virtual environment
----------------------------

You can zip the virtual environment on your own or use tools for doing this:

* `conda-pack <https://conda.github.io/conda-pack/spark.html>`_ for conda environments
* `venv-pack <https://jcristharif.com/venv-pack/spark.html>`_ for virtual environments

Example with conda-pack:

.. code-block:: bash

  conda create -y -n conda_env -c conda-forge \
       pyspark==3.0.1 pyarrow==0.15.1 \
       pandas==0.25.3 conda-pack==0.4.0
  conda activate conda_env
  conda pack -f -o conda_env.tar.gz

Upload it to the Spark executors
--------------------------------

The unzip will be done by Spark when using target ``--archives`` option in spark-submit
or setting ``spark.yarn.dist.archives`` configuration.

Example with spark-submit on YARN:

.. code-block:: bash

  export PYSPARK_DRIVER_PYTHON=python
  export PYSPARK_PYTHON=./environment/bin/python
  spark-submit --master=yarn --deploy-mode client \
  --archives conda_env.tar.gz#environment \
  main.py

Example using SparkSession.builder on YARN:

.. code-block:: python

  os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
  builder = SparkSession.builder \
           .master("yarn") \
           .config("spark.yarn.dist.archives",
                   "conda_env.tar.gz#environment")
  spark = builder.getOrCreate()
  main(spark)


*********
Using PEX
*********

`PEX <https://github.com/pantsbuild/pex>`_ is a library for generating .pex (Python EXecutable) files.
A PEX file is a self contained executable Python environment. It can be seen as the Python equivalent of Java uber-JARs (aka fat JARs).

You need to build the PEX file somewhere with all your requirements and then upload it to each Spark executor.

Using the CLI to build the PEX file
-----------------------------------

.. code-block:: bash

  pex pyspark==3.0.1 pyarrow==0.15.1 pandas==0.25.3 -o myarchive.pex


Invoking the pex file will by default invoke the Python interpreter. pyarrow, pandas and pyspark will be included in the pex file.

.. code-block:: bash
  
  ./myarchive.pex
  Python 3.6.6 (default, Jan 26 2019, 16:53:05)
  (InteractiveConsole)
  >>> import pyarrow
  >>> import pandas
  >>> import pyspark
  >>>

This can also be done directly with the Python API. `More infos <https://pex.readthedocs.io/en/stable/buildingpex.html>`_ on how to build PEX files.

Upload it to the Spark executors
--------------------------------

The upload can be done by setting ``--files`` option in spark-submit or setting ``spark.files`` configuration (``spark.yarn.dist.files`` on YARN) 
and changing the ``PYSPARK_PYTHON`` environment variable to change the Python interpreter to the PEX executable on each executor.

Example with spark-submit on YARN:

.. code-block:: bash

  export PYSPARK_DRIVER_PYTHON=python
  export PYSPARK_PYTHON=./myarchive.pex
  spark-submit --master=yarn --deploy-mode client \
  --files myarchive.pex \
  main.py

Example using SparkSession.builder on YARN:

.. code-block:: python

  import os
  from pyspark.sql import SparkSession
  from main import main
  os.environ['PYSPARK_PYTHON']="./myarchive.pex"
  builder = SparkSession.builder
  builder.master("yarn") \
         .config("spark.submit.deployMode", "client") \
         .config("spark.yarn.dist.files", "myarchive.pex")
  spark = builder.getOrCreate()
  main(spark)

Notes
-----

The Python interpreter that has been used to generate the PEX file must be available on each executor. PEX doesn't include the Python interpreter.

In YARN cluster mode you may also need to set ``PYSPARK_PYTHON`` environment variable on the AppMaster ``--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./myarchive.pex``.

An end-to-end Docker example for deploying a standalone PySpark with ``SparkSession.builder`` and PEX
can be found `here <https://github.com/criteo/cluster-pack/blob/master/examples/spark-with-S3/README.md>`_ 
(it uses cluster-pack, a library on top of PEX that automatizes the the intermediate step of having to create & upload the PEX manually).


**********************************
Using Docker
**********************************

Deploy you Spark application as usual with docker containers (on Standalone, Mesos, Kubernetes) and install all required packages globally in the Docker container.


*****************************************************
What about the Spark JARs/Python code in itsself ?
*****************************************************

PySpark should be included in the PEX/zipped env in order to be shipped along the other libraries.

The Spark jars will be picked up:
- from ``SPARK_HOME`` if this environment variable is set
- from the pyspark package if ``SPARK_HOME`` is not set (it allows to consistently use same pyspark package)
- you can also set the ``spark.jars``, ``spark.yarn.dist.jars``, ``spark.yarn.dist.archives`` parameters to make them available directly on distributed storage
