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

=======================
Python to Spark Type Conversions
=======================

.. currentmodule:: pyspark.sql.types

All data types of Spark SQL are located in the package of `pyspark.sql.types`.
You can access them by doing:

.. code-block:: python

    from pyspark.sql.types import *

.. list-table::
    :header-rows: 1

    * - Data type
      - Value type in Python
      - API to access or create a data type
    * - **ByteType**
      - | int or long
        |
        | **Note:** Numbers will be converted to 1-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -128 to 127.
      - ByteType()
    * - **ShortType**
      - | int or long
        |
        | **Note:** Numbers will be converted to 2-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -32768 to 32767.
      - ShortType()
    * - **IntegerType**
      - int or long
      - IntegerType()
    * - **LongType**
      - | long
        |
        | **Note:** Numbers will be converted to 8-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -9223372036854775808 to 9223372036854775807. Otherwise, please convert data to decimal.Decimal and use DecimalType.
      - LongType()
    * - **FloatType**
      - | float
        |
        | **Note:** Numbers will be converted to 4-byte single-precision floating point numbers at runtime.
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
      - | ArrayType(*elementType*, [*containsNull*])
        |
        | **Note:** The default value of *containsNull* is True.
    * - **MapType**
      - dict
      - | MapType(*keyType*, *valueType*, [*valueContainsNull]*)
        |
        | **Note:** The default value of *valueContainsNull* is True.
    * - **StructType**
      - list or tuple
      - | StructType(*fields*)
        |
        | **Note:** *fields* is a Seq of StructFields. Also, two fields with the same name are not allowed.
    * - **StructField**
      - | The value type in Python of the data type of this field. For example, Int for a StructField with the data type IntegerType.
      - | StructField(*name*, *dataType*, [*nullable*])
        |
        | **Note:** The default value of *nullable* is True.