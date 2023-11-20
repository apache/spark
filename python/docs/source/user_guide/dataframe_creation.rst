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

==================
DataFrame creation
==================

.. currentmodule:: pyspark.sql

Basic data structures
---------------------

Pyspark provides an important class for handling data:

1. :class:`DataFrame`: a distributed collection of data grouped into named columns.

Creating through `createDataFrame`
----------------------------------

A PySpark :class:`DataFrame` can be created via :meth:`SparkSession.createDataFrame` typically by passing
a list of lists, tuples, dictionaries and :class:`Row`, a pandas :class:`pandas.DataFrame`
and an :class:`pyspark.RDD` consisting of such a list.
:meth:`SparkSession.createDataFrame` takes the `schema` argument to specify the schema of the DataFrame.
When it is omitted, PySpark infers the corresponding schema by taking a sample from the data.

Creating a PySpark :class:`DataFrame` from a list of lists
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    df = spark.createDataFrame([['Alice', 1], ['Bob', 5]])
    df

DataFrame[_1: string, _2: bigint]


Creating a PySpark :class:`DataFrame` from a list of tuples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    df = spark.createDataFrame([('Alice', 1), ('Bob', 5)])
    df

DataFrame[_1: string, _2: bigint]


Creating a PySpark :class:`DataFrame` from a list of dictionaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    df = spark.createDataFrame([{'name': 'Alice', 'age': 1}])
    df

DataFrame[age: bigint, name: string]


Creating a PySpark :class:`DataFrame` from a list of :class:`Row`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pyspark.sql import Row
    Person = Row('name', 'age')
    df = spark.createDataFrame([Person("Alice", 1), Person("Bob", 5)])
    df

DataFrame[name: string, age: bigint]


Creating a PySpark :class:`DataFrame` from a :class:`pandas.DataFrame`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import pandas as pd
    df = spark.createDataFrame(pd.DataFrame([[1, 2]]))
    df

DataFrame[0: bigint, 1: bigint]


Creating a PySpark :class:`DataFrame` from a :class:`numpy.ndarray`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import numpy as np
    import pandas as pd
    df = spark.createDataFrame(pd.DataFrame(data=np.array([[1, 2], [3, 4]]), columns=['a', 'b']))
    df

DataFrame[a: bigint, b: bigint]


Creating a PySpark :class:`DataFrame` from an :class:`pyspark.RDD`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pyspark.sql import Row
    rdd = spark.sparkContext.parallelize([Row(name = "Alice", age = 2), Row(name = "Bob", age = 5)])
    df = spark.createDataFrame(rdd)
    df

DataFrame[name: string, age: bigint]


Creating through `read.format(...).load(...)`
---------------------------------------------

Creating a PySpark :class:`DataFrame` by reading existing **json** format file data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    df = spark.read.format("json").load("python/test_support/sql/people.json")
    df

DataFrame[age: bigint, name: string]


Creating a PySpark :class:`DataFrame` by reading existing **csv** format file data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    df = spark.read.format("csv").load("python/test_support/sql/people.csv")
    df

DataFrame[age: bigint, name: string]


Creating a PySpark :class:`DataFrame` by reading existing **parquet** format file data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    df = spark.read.format("parquet").load("python/test_support/sql/people.parquet")
    df

DataFrame[age: bigint, name: string]

Creating a PySpark :class:`DataFrame` by reading existing **orc** format file data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    df = spark.read.format("parquet").load("python/test_support/sql/people.orc")
    df

DataFrame[age: bigint, name: string]


Creating a PySpark :class:`DataFrame` by reading data from other databases using **JDBC**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    df = spark.read.format("jdbc").options(url=url, dbtable=dbtable).load()
    df

DataFrame[age: bigint, name: string]

