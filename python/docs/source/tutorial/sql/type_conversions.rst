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

================================
Python to Spark Type Conversions
================================

.. TODO: Add additional information on conversions when Arrow is enabled.
.. TODO: Add in-depth explanation and table for type conversions (SPARK-44734).

.. currentmodule:: pyspark.sql.types

When working with PySpark, you will often need to consider the conversions between Python-native
objects to their Spark equivalents. For instance, when working with user-defined functions, the
function return type will be cast by Spark to an appropriate Spark SQL type. Or, when creating a
``DataFrame``, you may supply ``numpy`` or ``pandas`` objects as the inputted data. This guide will cover
the various conversions between Python and Spark SQL types.

Browsing Type Conversions
-------------------------

Though this document provides a comprehensive list of type conversions, you may find it easier to
interactively check the conversion behavior of Spark. To do so, you can test small examples of
user-defined functions, and use the ``spark.createDataFrame`` interface.

All data types of Spark SQL are located in the package of ``pyspark.sql.types``.
You can access them by doing:

.. code-block:: python

    from pyspark.sql.types import *

Configuration
-------------
There are several configurations that affect the behavior of type conversions. These configurations
are listed below:

.. list-table::
    :header-rows: 1

    * - Configuration
      - Description
      - Default
    * - spark.sql.execution.pythonUDF.arrow.enabled
      - Enable PyArrow in PySpark. See more `here <arrow_pandas.rst>`_.
      - False
    * - spark.sql.pyspark.inferNestedDictAsStruct.enabled
      - When enabled, nested dictionaries are inferred as StructType. Otherwise, they are inferred as MapType.
      - False
    * - spark.sql.timestampType
      - If set to `TIMESTAMP_NTZ`, the default timestamp type is ``TimestampNTZType``. Otherwise, the default timestamp type is TimestampType.
      - ""
    * - spark.sql.execution.pandas.inferPandasDictAsMap
      - When enabled, Pandas dictionaries are inferred as MapType. Otherwise, they are inferred as StructType.
      - False

All Conversions
---------------
.. list-table::
    :header-rows: 1

    * - Data type
      - Value type in Python
      - API to access or create a data type
    * - **ByteType**
      - int
          .. note:: Numbers will be converted to 1-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -128 to 127.
      - ByteType()
    * - **ShortType**
      - int
          .. note:: Numbers will be converted to 2-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -32768 to 32767.
      - ShortType()
    * - **IntegerType**
      - int
      - IntegerType()
    * - **LongType**
      - int
          .. note:: Numbers will be converted to 8-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -9223372036854775808 to 9223372036854775807. Otherwise, please convert data to decimal.Decimal and use DecimalType.
      - LongType()
    * - **FloatType**
      - float
          .. note:: Numbers will be converted to 4-byte single-precision floating point numbers at runtime.
      - FloatType()
    * - **DoubleType**
      - float
      - DoubleType()
    * - **DecimalType**
      - decimal.Decimal
      - DecimalType()|
    * - **StringType**
      - string
      - StringType()
    * - **BinaryType**
      - bytearray
      - BinaryType()
    * - **BooleanType**
      - bool
      - BooleanType()
    * - **TimestampType**
      - datetime.datetime
      - TimestampType()
    * - **TimestampNTZType**
      - datetime.datetime
      - TimestampNTZType()
    * - **DateType**
      - datetime.date
      - DateType()
    * - **DayTimeIntervalType**
      - datetime.timedelta
      - DayTimeIntervalType()
    * - **ArrayType**
      - list, tuple, or array
      - ArrayType(*elementType*, [*containsNull*])
          .. note:: The default value of *containsNull* is True.
    * - **MapType**
      - dict
      - MapType(*keyType*, *valueType*, [*valueContainsNull]*)
          .. note:: The default value of *valueContainsNull* is True.
    * - **StructType**
      - list or tuple
      - StructType(*fields*)
          .. note:: *fields* is a Seq of StructFields. Also, two fields with the same name are not allowed.
    * - **StructField**
      - The value type in Python of the data type of this field. For example, Int for a StructField with the data type IntegerType.
      - StructField(*name*, *dataType*, [*nullable*])
          .. note:: The default value of *nullable* is True.

Conversions in Practice - UDFs
------------------------------
A common conversion case is returning a Python value from a UDF. In this case, the return type of
the UDF must match the provided return type.

.. note:: If the actual return type of your function does not match the provided return type, Spark will implicitly cast the value to null.

.. code-block:: python

  from pyspark.sql.types import (
      StructType,
      StructField,
      IntegerType,
      StringType,
      FloatType,
  )
  from pyspark.sql.functions import udf, col

  df = spark.createDataFrame(
      [[1]], schema=StructType([StructField("int", IntegerType())])
  )

  @udf(returnType=StringType())
  def to_string(value):
      return str(value)

  @udf(returnType=FloatType())
  def to_float(value):
      return float(value)

  df.withColumn("cast_int", to_float(col("int"))).withColumn(
      "cast_str", to_string(col("int"))
  ).printSchema()
  # root
  # |-- int: integer (nullable = true)
  # |-- cast_int: float (nullable = true)
  # |-- cast_str: string (nullable = true)

Conversions in Practice - Creating DataFrames
---------------------------------------------
Another common conversion case is when creating a DataFrame from values in Python. In this case,
you can supply a schema, or allow Spark to infer the schema from the provided data.

.. code-block:: python

  data = [
      ["Wei", "Math", 93.0, 1],
      ["Jerry", "Physics", 85.0, 4],
      ["Katrina", "Geology", 90.0, 2],
  ]
  cols = ["Name", "Subject", "Score", "Period"]

  spark.createDataFrame(data, cols).printSchema()
  # root
  # |-- Name: string (nullable = true)
  # |-- Subject: string (nullable = true)
  # |-- Score: double (nullable = true)
  # |-- Period: long (nullable = true)

  import pandas as pd

  df = pd.DataFrame(data, columns=cols)
  spark.createDataFrame(df).printSchema()
  # root
  # |-- Name: string (nullable = true)
  # |-- Subject: string (nullable = true)
  # |-- Score: double (nullable = true)
  # |-- Period: long (nullable = true)

  import numpy as np

  spark.createDataFrame(np.zeros([3, 2], "int8")).printSchema()
  # root
  # |-- _1: byte (nullable = true)
  # |-- _2: byte (nullable = true)

Conversions in Practice - Nested Data Types
-------------------------------------------
Nested data types will convert to ``StructType``, ``MapType``, and ``ArrayType``, depending on the passed data.

.. code-block:: python

  data = [
      ["Wei", [[1, 2]], {"RecordType": "Scores", "Math": { "H1": 93.0, "H2": 85.0}}],
  ]
  cols = ["Name", "ActiveHalfs", "Record"]

  spark.createDataFrame(data, cols).printSchema()
  # root
  #  |-- Name: string (nullable = true)
  #  |-- ActiveHalfs: array (nullable = true)
  #  |    |-- element: array (containsNull = true)
  #  |    |    |-- element: long (containsNull = true)
  #  |-- Record: map (nullable = true)
  #  |    |-- key: string
  #  |    |-- value: string (valueContainsNull = true)

  spark.conf.set('spark.sql.pyspark.inferNestedDictAsStruct.enabled', True)

  spark.createDataFrame(data, cols).printSchema()
  # root
  #  |-- Name: string (nullable = true)
  #  |-- ActiveHalfs: array (nullable = true)
  #  |    |-- element: array (containsNull = true)
  #  |    |    |-- element: long (containsNull = true)
  #  |-- Record: struct (nullable = true)
  #  |    |-- RecordType: string (nullable = true)
  #  |    |-- Math: struct (nullable = true)
  #  |    |    |-- H1: double (nullable = true)
  #  |    |    |-- H2: double (nullable = true)
