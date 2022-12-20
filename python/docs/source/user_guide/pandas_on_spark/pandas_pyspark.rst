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


=====================================
From/to pandas and PySpark DataFrames
=====================================

.. currentmodule:: pyspark.pandas

Users from pandas and/or PySpark face API compatibility issue sometimes when they
work with pandas API on Spark. Since pandas API on Spark does not target 100% compatibility of both pandas and
PySpark, users need to do some workaround to port their pandas and/or PySpark codes or
get familiar with pandas API on Spark in this case. This page aims to describe it.


pandas
------

pandas users can access the full pandas API by calling :func:`DataFrame.to_pandas`.
pandas-on-Spark DataFrame and pandas DataFrame are similar. However, the former is distributed
and the latter is in a single machine. When converting to each other, the data is
transferred between multiple machines and the single client machine.

For example, if you need to call ``pandas_df.values`` of pandas DataFrame, you can do
as below:

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>>
   >>> psdf = ps.range(10)
   >>> pdf = psdf.to_pandas()
   >>> pdf.values
   array([[0],
          [1],
          [2],
          [3],
          [4],
          [5],
          [6],
          [7],
          [8],
          [9]])

pandas DataFrame can be a pandas-on-Spark DataFrame easily as below:

.. code-block:: python

   >>> ps.from_pandas(pdf)
      id
   0   0
   1   1
   2   2
   3   3
   4   4
   5   5
   6   6
   7   7
   8   8
   9   9

Note that converting pandas-on-Spark DataFrame to pandas requires to collect all the data into the client machine; therefore,
if possible, it is recommended to use pandas API on Spark or PySpark APIs instead.


PySpark
-------

PySpark users can access to full PySpark APIs by calling :func:`DataFrame.to_spark`.
pandas-on-Spark DataFrame and Spark DataFrame are virtually interchangeable.

For example, if you need to call ``spark_df.filter(...)`` of Spark DataFrame, you can do
as below:

.. code-block:: python

   >>> import pyspark.pandas as ps
   >>>
   >>> psdf = ps.range(10)
   >>> sdf = psdf.to_spark().filter("id > 5")
   >>> sdf.show()
   +---+
   | id|
   +---+
   |  6|
   |  7|
   |  8|
   |  9|
   +---+

Spark DataFrame can be a pandas-on-Spark DataFrame easily as below:

.. code-block:: python

   >>> sdf.to_pandas_on_spark()
      id
   0   6
   1   7
   2   8
   3   9

However, note that a new default index is created when pandas-on-Spark DataFrame is created from
Spark DataFrame. See `Default Index Type <options.rst#default-index-type>`_. In order to avoid this overhead, specify the column
to use as an index when possible.

.. code-block:: python

   >>> # Create a pandas-on-Spark DataFrame with an explicit index.
   ... psdf = ps.DataFrame({'id': range(10)}, index=range(10))
   >>> # Keep the explicit index.
   ... sdf = psdf.to_spark(index_col='index')
   >>> # Call Spark APIs
   ... sdf = sdf.filter("id > 5")
   >>> # Uses the explicit index to avoid to create default index.
   ... sdf.to_pandas_on_spark(index_col='index')
          id
   index
   6       6
   7       7
   8       8
   9       9
