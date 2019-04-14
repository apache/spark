---
layout: global
title: Reference
displayTitle: Reference
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}

## Data Types

Spark SQL and DataFrames support the following data types:

* Numeric types
  - `ByteType`: Represents 1-byte signed integer numbers.
  The range of numbers is from `-128` to `127`.
  - `ShortType`: Represents 2-byte signed integer numbers.
  The range of numbers is from `-32768` to `32767`.
  - `IntegerType`: Represents 4-byte signed integer numbers.
  The range of numbers is from `-2147483648` to `2147483647`.
  - `LongType`: Represents 8-byte signed integer numbers.
  The range of numbers is from `-9223372036854775808` to `9223372036854775807`.
  - `FloatType`: Represents 4-byte single-precision floating point numbers.
  - `DoubleType`: Represents 8-byte double-precision floating point numbers.
  - `DecimalType`: Represents arbitrary-precision signed decimal numbers. Backed internally by `java.math.BigDecimal`. A `BigDecimal` consists of an arbitrary precision integer unscaled value and a 32-bit integer scale.
* String type
  - `StringType`: Represents character string values.
* Binary type
  - `BinaryType`: Represents byte sequence values.
* Boolean type
  - `BooleanType`: Represents boolean values.
* Datetime type
  - `TimestampType`: Represents values comprising values of fields year, month, day,
  hour, minute, and second, with the session local time-zone. The timestamp value represents an
  absolute point in time.
  - `DateType`: Represents values comprising values of fields year, month and day, without a
  time-zone.
* Complex types
  - `ArrayType(elementType, containsNull)`: Represents values comprising a sequence of
  elements with the type of `elementType`. `containsNull` is used to indicate if
  elements in a `ArrayType` value can have `null` values.
  - `MapType(keyType, valueType, valueContainsNull)`:
  Represents values comprising a set of key-value pairs. The data type of keys is
  described by `keyType` and the data type of values is described by `valueType`.
  For a `MapType` value, keys are not allowed to have `null` values. `valueContainsNull`
  is used to indicate if values of a `MapType` value can have `null` values.
  - `StructType(fields)`: Represents values with the structure described by
  a sequence of `StructField`s (`fields`).
    * `StructField(name, dataType, nullable)`: Represents a field in a `StructType`.
    The name of a field is indicated by `name`. The data type of a field is indicated
    by `dataType`. `nullable` is used to indicate if values of these fields can have
    `null` values.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

All data types of Spark SQL are located in the package `org.apache.spark.sql.types`.
You can access them by doing

{% include_example data_types scala/org/apache/spark/examples/sql/SparkSQLExample.scala %}

<table class="table">
<tr>
  <th style="width:20%">Data type</th>
  <th style="width:40%">Value type in Scala</th>
  <th>API to access or create a data type</th></tr>
<tr>
  <td> <b>ByteType</b> </td>
  <td> Byte </td>
  <td>
  ByteType
  </td>
</tr>
<tr>
  <td> <b>ShortType</b> </td>
  <td> Short </td>
  <td>
  ShortType
  </td>
</tr>
<tr>
  <td> <b>IntegerType</b> </td>
  <td> Int </td>
  <td>
  IntegerType
  </td>
</tr>
<tr>
  <td> <b>LongType</b> </td>
  <td> Long </td>
  <td>
  LongType
  </td>
</tr>
<tr>
  <td> <b>FloatType</b> </td>
  <td> Float </td>
  <td>
  FloatType
  </td>
</tr>
<tr>
  <td> <b>DoubleType</b> </td>
  <td> Double </td>
  <td>
  DoubleType
  </td>
</tr>
<tr>
  <td> <b>DecimalType</b> </td>
  <td> java.math.BigDecimal </td>
  <td>
  DecimalType
  </td>
</tr>
<tr>
  <td> <b>StringType</b> </td>
  <td> String </td>
  <td>
  StringType
  </td>
</tr>
<tr>
  <td> <b>BinaryType</b> </td>
  <td> Array[Byte] </td>
  <td>
  BinaryType
  </td>
</tr>
<tr>
  <td> <b>BooleanType</b> </td>
  <td> Boolean </td>
  <td>
  BooleanType
  </td>
</tr>
<tr>
  <td> <b>TimestampType</b> </td>
  <td> java.sql.Timestamp </td>
  <td>
  TimestampType
  </td>
</tr>
<tr>
  <td> <b>DateType</b> </td>
  <td> java.sql.Date </td>
  <td>
  DateType
  </td>
</tr>
<tr>
  <td> <b>ArrayType</b> </td>
  <td> scala.collection.Seq </td>
  <td>
  ArrayType(<i>elementType</i>, [<i>containsNull</i>])<br />
  <b>Note:</b> The default value of <i>containsNull</i> is <i>true</i>.
  </td>
</tr>
<tr>
  <td> <b>MapType</b> </td>
  <td> scala.collection.Map </td>
  <td>
  MapType(<i>keyType</i>, <i>valueType</i>, [<i>valueContainsNull</i>])<br />
  <b>Note:</b> The default value of <i>valueContainsNull</i> is <i>true</i>.
  </td>
</tr>
<tr>
  <td> <b>StructType</b> </td>
  <td> org.apache.spark.sql.Row </td>
  <td>
  StructType(<i>fields</i>)<br />
  <b>Note:</b> <i>fields</i> is a Seq of StructFields. Also, two fields with the same
  name are not allowed.
  </td>
</tr>
<tr>
  <td> <b>StructField</b> </td>
  <td> The value type in Scala of the data type of this field
  (For example, Int for a StructField with the data type IntegerType) </td>
  <td>
  StructField(<i>name</i>, <i>dataType</i>, [<i>nullable</i>])<br />
  <b>Note:</b> The default value of <i>nullable</i> is <i>true</i>.
  </td>
</tr>
</table>

</div>

<div data-lang="java" markdown="1">

All data types of Spark SQL are located in the package of
`org.apache.spark.sql.types`. To access or create a data type,
please use factory methods provided in
`org.apache.spark.sql.types.DataTypes`.

<table class="table">
<tr>
  <th style="width:20%">Data type</th>
  <th style="width:40%">Value type in Java</th>
  <th>API to access or create a data type</th></tr>
<tr>
  <td> <b>ByteType</b> </td>
  <td> byte or Byte </td>
  <td>
  DataTypes.ByteType
  </td>
</tr>
<tr>
  <td> <b>ShortType</b> </td>
  <td> short or Short </td>
  <td>
  DataTypes.ShortType
  </td>
</tr>
<tr>
  <td> <b>IntegerType</b> </td>
  <td> int or Integer </td>
  <td>
  DataTypes.IntegerType
  </td>
</tr>
<tr>
  <td> <b>LongType</b> </td>
  <td> long or Long </td>
  <td>
  DataTypes.LongType
  </td>
</tr>
<tr>
  <td> <b>FloatType</b> </td>
  <td> float or Float </td>
  <td>
  DataTypes.FloatType
  </td>
</tr>
<tr>
  <td> <b>DoubleType</b> </td>
  <td> double or Double </td>
  <td>
  DataTypes.DoubleType
  </td>
</tr>
<tr>
  <td> <b>DecimalType</b> </td>
  <td> java.math.BigDecimal </td>
  <td>
  DataTypes.createDecimalType()<br />
  DataTypes.createDecimalType(<i>precision</i>, <i>scale</i>).
  </td>
</tr>
<tr>
  <td> <b>StringType</b> </td>
  <td> String </td>
  <td>
  DataTypes.StringType
  </td>
</tr>
<tr>
  <td> <b>BinaryType</b> </td>
  <td> byte[] </td>
  <td>
  DataTypes.BinaryType
  </td>
</tr>
<tr>
  <td> <b>BooleanType</b> </td>
  <td> boolean or Boolean </td>
  <td>
  DataTypes.BooleanType
  </td>
</tr>
<tr>
  <td> <b>TimestampType</b> </td>
  <td> java.sql.Timestamp </td>
  <td>
  DataTypes.TimestampType
  </td>
</tr>
<tr>
  <td> <b>DateType</b> </td>
  <td> java.sql.Date </td>
  <td>
  DataTypes.DateType
  </td>
</tr>
<tr>
  <td> <b>ArrayType</b> </td>
  <td> java.util.List </td>
  <td>
  DataTypes.createArrayType(<i>elementType</i>)<br />
  <b>Note:</b> The value of <i>containsNull</i> will be <i>true</i><br />
  DataTypes.createArrayType(<i>elementType</i>, <i>containsNull</i>).
  </td>
</tr>
<tr>
  <td> <b>MapType</b> </td>
  <td> java.util.Map </td>
  <td>
  DataTypes.createMapType(<i>keyType</i>, <i>valueType</i>)<br />
  <b>Note:</b> The value of <i>valueContainsNull</i> will be <i>true</i>.<br />
  DataTypes.createMapType(<i>keyType</i>, <i>valueType</i>, <i>valueContainsNull</i>)<br />
  </td>
</tr>
<tr>
  <td> <b>StructType</b> </td>
  <td> org.apache.spark.sql.Row </td>
  <td>
  DataTypes.createStructType(<i>fields</i>)<br />
  <b>Note:</b> <i>fields</i> is a List or an array of StructFields.
  Also, two fields with the same name are not allowed.
  </td>
</tr>
<tr>
  <td> <b>StructField</b> </td>
  <td> The value type in Java of the data type of this field
  (For example, int for a StructField with the data type IntegerType) </td>
  <td>
  DataTypes.createStructField(<i>name</i>, <i>dataType</i>, <i>nullable</i>)
  </td>
</tr>
</table>

</div>

<div data-lang="python"  markdown="1">

All data types of Spark SQL are located in the package of `pyspark.sql.types`.
You can access them by doing
{% highlight python %}
from pyspark.sql.types import *
{% endhighlight %}

<table class="table">
<tr>
  <th style="width:20%">Data type</th>
  <th style="width:40%">Value type in Python</th>
  <th>API to access or create a data type</th></tr>
<tr>
  <td> <b>ByteType</b> </td>
  <td>
  int or long <br />
  <b>Note:</b> Numbers will be converted to 1-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of -128 to 127.
  </td>
  <td>
  ByteType()
  </td>
</tr>
<tr>
  <td> <b>ShortType</b> </td>
  <td>
  int or long <br />
  <b>Note:</b> Numbers will be converted to 2-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of -32768 to 32767.
  </td>
  <td>
  ShortType()
  </td>
</tr>
<tr>
  <td> <b>IntegerType</b> </td>
  <td> int or long </td>
  <td>
  IntegerType()
  </td>
</tr>
<tr>
  <td> <b>LongType</b> </td>
  <td>
  long <br />
  <b>Note:</b> Numbers will be converted to 8-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of
  -9223372036854775808 to 9223372036854775807.
  Otherwise, please convert data to decimal.Decimal and use DecimalType.
  </td>
  <td>
  LongType()
  </td>
</tr>
<tr>
  <td> <b>FloatType</b> </td>
  <td>
  float <br />
  <b>Note:</b> Numbers will be converted to 4-byte single-precision floating
  point numbers at runtime.
  </td>
  <td>
  FloatType()
  </td>
</tr>
<tr>
  <td> <b>DoubleType</b> </td>
  <td> float </td>
  <td>
  DoubleType()
  </td>
</tr>
<tr>
  <td> <b>DecimalType</b> </td>
  <td> decimal.Decimal </td>
  <td>
  DecimalType()
  </td>
</tr>
<tr>
  <td> <b>StringType</b> </td>
  <td> string </td>
  <td>
  StringType()
  </td>
</tr>
<tr>
  <td> <b>BinaryType</b> </td>
  <td> bytearray </td>
  <td>
  BinaryType()
  </td>
</tr>
<tr>
  <td> <b>BooleanType</b> </td>
  <td> bool </td>
  <td>
  BooleanType()
  </td>
</tr>
<tr>
  <td> <b>TimestampType</b> </td>
  <td> datetime.datetime </td>
  <td>
  TimestampType()
  </td>
</tr>
<tr>
  <td> <b>DateType</b> </td>
  <td> datetime.date </td>
  <td>
  DateType()
  </td>
</tr>
<tr>
  <td> <b>ArrayType</b> </td>
  <td> list, tuple, or array </td>
  <td>
  ArrayType(<i>elementType</i>, [<i>containsNull</i>])<br />
  <b>Note:</b> The default value of <i>containsNull</i> is <i>True</i>.
  </td>
</tr>
<tr>
  <td> <b>MapType</b> </td>
  <td> dict </td>
  <td>
  MapType(<i>keyType</i>, <i>valueType</i>, [<i>valueContainsNull</i>])<br />
  <b>Note:</b> The default value of <i>valueContainsNull</i> is <i>True</i>.
  </td>
</tr>
<tr>
  <td> <b>StructType</b> </td>
  <td> list or tuple </td>
  <td>
  StructType(<i>fields</i>)<br />
  <b>Note:</b> <i>fields</i> is a Seq of StructFields. Also, two fields with the same
  name are not allowed.
  </td>
</tr>
<tr>
  <td> <b>StructField</b> </td>
  <td> The value type in Python of the data type of this field
  (For example, Int for a StructField with the data type IntegerType) </td>
  <td>
  StructField(<i>name</i>, <i>dataType</i>, [<i>nullable</i>])<br />
  <b>Note:</b> The default value of <i>nullable</i> is <i>True</i>.
  </td>
</tr>
</table>

</div>

<div data-lang="r"  markdown="1">

<table class="table">
<tr>
  <th style="width:20%">Data type</th>
  <th style="width:40%">Value type in R</th>
  <th>API to access or create a data type</th></tr>
<tr>
  <td> <b>ByteType</b> </td>
  <td>
  integer <br />
  <b>Note:</b> Numbers will be converted to 1-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of -128 to 127.
  </td>
  <td>
  "byte"
  </td>
</tr>
<tr>
  <td> <b>ShortType</b> </td>
  <td>
  integer <br />
  <b>Note:</b> Numbers will be converted to 2-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of -32768 to 32767.
  </td>
  <td>
  "short"
  </td>
</tr>
<tr>
  <td> <b>IntegerType</b> </td>
  <td> integer </td>
  <td>
  "integer"
  </td>
</tr>
<tr>
  <td> <b>LongType</b> </td>
  <td>
  integer <br />
  <b>Note:</b> Numbers will be converted to 8-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of
  -9223372036854775808 to 9223372036854775807.
  Otherwise, please convert data to decimal.Decimal and use DecimalType.
  </td>
  <td>
  "long"
  </td>
</tr>
<tr>
  <td> <b>FloatType</b> </td>
  <td>
  numeric <br />
  <b>Note:</b> Numbers will be converted to 4-byte single-precision floating
  point numbers at runtime.
  </td>
  <td>
  "float"
  </td>
</tr>
<tr>
  <td> <b>DoubleType</b> </td>
  <td> numeric </td>
  <td>
  "double"
  </td>
</tr>
<tr>
  <td> <b>DecimalType</b> </td>
  <td> Not supported </td>
  <td>
   Not supported
  </td>
</tr>
<tr>
  <td> <b>StringType</b> </td>
  <td> character </td>
  <td>
  "string"
  </td>
</tr>
<tr>
  <td> <b>BinaryType</b> </td>
  <td> raw </td>
  <td>
  "binary"
  </td>
</tr>
<tr>
  <td> <b>BooleanType</b> </td>
  <td> logical </td>
  <td>
  "bool"
  </td>
</tr>
<tr>
  <td> <b>TimestampType</b> </td>
  <td> POSIXct </td>
  <td>
  "timestamp"
  </td>
</tr>
<tr>
  <td> <b>DateType</b> </td>
  <td> Date </td>
  <td>
  "date"
  </td>
</tr>
<tr>
  <td> <b>ArrayType</b> </td>
  <td> vector or list </td>
  <td>
  list(type="array", elementType=<i>elementType</i>, containsNull=[<i>containsNull</i>])<br />
  <b>Note:</b> The default value of <i>containsNull</i> is <i>TRUE</i>.
  </td>
</tr>
<tr>
  <td> <b>MapType</b> </td>
  <td> environment </td>
  <td>
  list(type="map", keyType=<i>keyType</i>, valueType=<i>valueType</i>, valueContainsNull=[<i>valueContainsNull</i>])<br />
  <b>Note:</b> The default value of <i>valueContainsNull</i> is <i>TRUE</i>.
  </td>
</tr>
<tr>
  <td> <b>StructType</b> </td>
  <td> named list</td>
  <td>
  list(type="struct", fields=<i>fields</i>)<br />
  <b>Note:</b> <i>fields</i> is a Seq of StructFields. Also, two fields with the same
  name are not allowed.
  </td>
</tr>
<tr>
  <td> <b>StructField</b> </td>
  <td> The value type in R of the data type of this field
  (For example, integer for a StructField with the data type IntegerType) </td>
  <td>
  list(name=<i>name</i>, type=<i>dataType</i>, nullable=[<i>nullable</i>])<br />
  <b>Note:</b> The default value of <i>nullable</i> is <i>TRUE</i>.
  </td>
</tr>
</table>

</div>

</div>

## NaN Semantics

There is specially handling for not-a-number (NaN) when dealing with `float` or `double` types that
does not exactly match standard floating point semantics.
Specifically:

 - NaN = NaN returns true.
 - In aggregations, all NaN values are grouped together.
 - NaN is treated as a normal value in join keys.
 - NaN values go last when in ascending order, larger than any other numeric value.
 
## Arithmetic operations
 
Operations performed on numeric types (with the exception of `decimal`) are not checked for overflow.
This means that in case an operation causes an overflow, the result is the same that the same operation
returns in a Java/Scala program (eg. if the sum of 2 integers is higher than the maximum value representable,
the result is a negative number).
