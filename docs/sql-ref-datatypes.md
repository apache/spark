---
layout: global
title: Data Types
displayTitle: Data Types
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

### Supported Data Types

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
  - `VarcharType(length)`: A variant of `StringType` which has a length limitation. Data writing will fail if the input string exceeds the length limitation. Note: this type can only be used in table schema, not functions/operators.
  - `CharType(length)`: A variant of `VarcharType(length)` which is fixed length. Reading column of type `CharType(n)` always returns string values of length `n`. Char type column comparison will pad the short one to the longer length.
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
* Interval types
  - `YearMonthIntervalType(startField, endField)`: Represents a year-month interval which is made up of a contiguous subset of the following fields:
    - MONTH, months within years `[0..11]`,
    - YEAR, years in the range `[0..178956970]`.

    Individual interval fields are non-negative, but an interval itself can have a sign, and be negative.

    `startField` is the leftmost field, and `endField` is the rightmost field of the type. Valid values of `startField` and `endField` are 0(MONTH) and 1(YEAR). Supported year-month interval types are:
    
    |Year-Month Interval Type|SQL type|An instance of the type|
    |---------|----|-------------------|
    |`YearMonthIntervalType(YEAR, YEAR)` or `YearMonthIntervalType(YEAR)`|INTERVAL YEAR|`INTERVAL '2021' YEAR`|
    |`YearMonthIntervalType(YEAR, MONTH)`|INTERVAL YEAR TO MONTH|`INTERVAL '2021-07' YEAR TO MONTH`|
    |`YearMonthIntervalType(MONTH, MONTH)` or `YearMonthIntervalType(MONTH)`|INTERVAL MONTH|`INTERVAL '10' MONTH`|

  - `DayTimeIntervalType(startField, endField)`: Represents a day-time interval which is made up of a contiguous subset of the following fields:
    - SECOND, seconds within minutes and possibly fractions of a second `[0..59.999999]`,
    - MINUTE, minutes within hours `[0..59]`,
    - HOUR, hours within days `[0..23]`,
    - DAY, days in the range `[0..106751991]`.

    Individual interval fields are non-negative, but an interval itself can have a sign, and be negative.

    `startField` is the leftmost field, and `endField` is the rightmost field of the type. Valid values of `startField` and `endField` are 0 (DAY), 1 (HOUR), 2 (MINUTE), 3 (SECOND). Supported day-time interval types are:

    |Day-Time Interval Type|SQL type|An instance of the type|
    |---------|----|-------------------|
    |`DayTimeIntervalType(DAY, DAY)` or `DayTimeIntervalType(DAY)`|INTERVAL DAY|`INTERVAL '100' DAY`|
    |`DayTimeIntervalType(DAY, HOUR)`|INTERVAL DAY TO HOUR|`INTERVAL '100 10' DAY TO HOUR`|
    |`DayTimeIntervalType(DAY, MINUTE)`|INTERVAL DAY TO MINUTE|`INTERVAL '100 10:30' DAY TO MINUTE`|
    |`DayTimeIntervalType(DAY, SECOND)`|INTERVAL DAY TO SECOND|`INTERVAL '100 10:30:40.999999' DAY TO SECOND`|
    |`DayTimeIntervalType(HOUR, HOUR)` or `DayTimeIntervalType(HOUR)`|INTERVAL HOUR|`INTERVAL '123' HOUR`|
    |`DayTimeIntervalType(HOUR, MINUTE)`|INTERVAL HOUR TO MINUTE|`INTERVAL '123:10' HOUR TO MINUTE`|
    |`DayTimeIntervalType(HOUR, SECOND)`|INTERVAL HOUR TO SECOND|`INTERVAL '123:10:59' HOUR TO SECOND`|
    |`DayTimeIntervalType(MINUTE, MINUTE)` or `DayTimeIntervalType(MINUTE)`|INTERVAL MINUTE|`INTERVAL '1000' MINUTE`|
    |`DayTimeIntervalType(MINUTE, SECOND)`|INTERVAL MINUTE TO SECOND|`INTERVAL '1000:01.001' MINUTE TO SECOND`|
    |`DayTimeIntervalType(SECOND, SECOND)` or `DayTimeIntervalType(SECOND)`|INTERVAL SECOND|`INTERVAL '1000.000001' SECOND`|

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

|Data type|Value type in Scala|API to access or create a data type|
|---------|-------------------|-----------------------------------|
|**ByteType**|Byte|ByteType|
|**ShortType**|Short|ShortType|
|**IntegerType**|Int|IntegerType|
|**LongType**|Long|LongType|
|**FloatType**|Float|FloatType|
|**DoubleType**|Double|DoubleType|
|**DecimalType**|java.math.BigDecimal|DecimalType|
|**StringType**|String|StringType|
|**BinaryType**|Array[Byte]|BinaryType|
|**BooleanType**|Boolean|BooleanType|
|**TimestampType**|java.sql.Timestamp|TimestampType|
|**DateType**|java.sql.Date|DateType|
|**YearMonthIntervalType**|java.time.Period|YearMonthIntervalType|
|**DayTimeIntervalType**|java.time.Duration|DayTimeIntervalType|
|**ArrayType**|scala.collection.Seq|ArrayType(*elementType*, [*containsNull]*)<br/>**Note:** The default value of *containsNull* is true.|
|**MapType**|scala.collection.Map|MapType(*keyType*, *valueType*, [*valueContainsNull]*)<br/>**Note:** The default value of *valueContainsNull* is true.|
|**StructType**|org.apache.spark.sql.Row|StructType(*fields*)<br/>**Note:** *fields* is a Seq of StructFields. Also, two fields with the same name are not allowed.|
|**StructField**|The value type in Scala of the data type of this field(For example, Int for a StructField with the data type IntegerType)|StructField(*name*, *dataType*, [*nullable*])<br/>**Note:** The default value of *nullable* is true.|

</div>

<div data-lang="java" markdown="1">

All data types of Spark SQL are located in the package of
`org.apache.spark.sql.types`. To access or create a data type,
please use factory methods provided in
`org.apache.spark.sql.types.DataTypes`.

|Data type|Value type in Java|API to access or create a data type|
|---------|------------------|-----------------------------------|
|**ByteType**|byte or Byte|DataTypes.ByteType|
|**ShortType**|short or Short|DataTypes.ShortType|
|**IntegerType**|int or Integer|DataTypes.IntegerType|
|**LongType**|long or Long|DataTypes.LongType|
|**FloatType**|float or Float|DataTypes.FloatType|
|**DoubleType**|double or Double|DataTypes.DoubleType|
|**DecimalType**|java.math.BigDecimal|DataTypes.createDecimalType()<br/>DataTypes.createDecimalType(*precision*, *scale*).|
|**StringType**|String|DataTypes.StringType|
|**BinaryType**|byte[]|DataTypes.BinaryType|
|**BooleanType**|boolean or Boolean|DataTypes.BooleanType|
|**TimestampType**|java.sql.Timestamp|DataTypes.TimestampType|
|**DateType**|java.sql.Date|DataTypes.DateType|
|**YearMonthIntervalType**|java.time.Period|YearMonthIntervalType|
|**DayTimeIntervalType**|java.time.Duration|DayTimeIntervalType|
|**ArrayType**|java.util.List|DataTypes.createArrayType(*elementType*)<br/>**Note:** The value of *containsNull* will be true.<br/>DataTypes.createArrayType(*elementType*, *containsNull*).|
|**MapType**|java.util.Map|DataTypes.createMapType(*keyType*, *valueType*)<br/>**Note:** The value of *valueContainsNull* will be true.<br/>DataTypes.createMapType(*keyType*, *valueType*, *valueContainsNull*)|
|**StructType**|org.apache.spark.sql.Row|DataTypes.createStructType(*fields*)<br/>**Note:** *fields* is a List or an array of StructFields.Also, two fields with the same name are not allowed.|
|**StructField**|The value type in Java of the data type of this field (For example, int for a StructField with the data type IntegerType)|DataTypes.createStructField(*name*, *dataType*, *nullable*)| 

</div>

<div data-lang="python"  markdown="1">

All data types of Spark SQL are located in the package of `pyspark.sql.types`.
You can access them by doing
{% highlight python %}
from pyspark.sql.types import *
{% endhighlight %}

|Data type|Value type in Python|API to access or create a data type|
|---------|--------------------|-----------------------------------|
|**ByteType**|int or long<br/>**Note:** Numbers will be converted to 1-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -128 to 127.|ByteType()|
|**ShortType**|int or long<br/>**Note:** Numbers will be converted to 2-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -32768 to 32767.|ShortType()|
|**IntegerType**|int or long|IntegerType()|
|**LongType**|long<br/>**Note:** Numbers will be converted to 8-byte signed integer numbers at runtime. Please make sure that numbers are within the range of -9223372036854775808 to 9223372036854775807. Otherwise, please convert data to decimal.Decimal and use DecimalType.|LongType()|
|**FloatType**|float<br/>**Note:** Numbers will be converted to 4-byte single-precision floating point numbers at runtime.|FloatType()|
|**DoubleType**|float|DoubleType()|
|**DecimalType**|decimal.Decimal|DecimalType()|
|**StringType**|string|StringType()|
|**BinaryType**|bytearray|BinaryType()|
|**BooleanType**|bool|BooleanType()|
|**TimestampType**|datetime.datetime|TimestampType()|
|**DateType**|datetime.date|DateType()|
|**DayTimeIntervalType**|datetime.timedelta|DayTimeIntervalType()|
|**ArrayType**|list, tuple, or array|ArrayType(*elementType*, [*containsNull*])<br/>**Note:**The default value of *containsNull* is True.|
|**MapType**|dict|MapType(*keyType*, *valueType*, [*valueContainsNull]*)<br/>**Note:**The default value of *valueContainsNull* is True.|
|**StructType**|list or tuple|StructType(*fields*)<br/>**Note:** *fields* is a Seq of StructFields. Also, two fields with the same name are not allowed.|
|**StructField**|The value type in Python of the data type of this field<br/>(For example, Int for a StructField with the data type IntegerType)|StructField(*name*, *dataType*, [*nullable*])<br/>**Note:** The default value of *nullable* is True.|

</div>

<div data-lang="r"  markdown="1">

|Data type|Value type in R|API to access or create a data type|
|---------|---------------|-----------------------------------|
|**ByteType**|integer <br/>**Note:** Numbers will be converted to 1-byte signed integer numbers at runtime.  Please make sure that numbers are within the range of -128 to 127.|"byte"|
|**ShortType**|integer <br/>**Note:** Numbers will be converted to 2-byte signed integer numbers at runtime.  Please make sure that numbers are within the range of -32768 to 32767.|"short"|
|**IntegerType**|integer|"integer"|
|**LongType**|integer <br/>**Note:** Numbers will be converted to 8-byte signed integer numbers at runtime.  Please make sure that numbers are within the range of -9223372036854775808 to 9223372036854775807.  Otherwise, please convert data to decimal.Decimal and use DecimalType.|"long"|
|**FloatType**|numeric <br/>**Note:** Numbers will be converted to 4-byte single-precision floating point numbers at runtime.|"float"|
|**DoubleType**|numeric|"double"|
|**DecimalType**|Not supported|Not supported|
|**StringType**|character|"string"|
|**BinaryType**|raw|"binary"|
|**BooleanType**|logical|"bool"|
|**TimestampType**|POSIXct|"timestamp"|
|**DateType**|Date|"date"|
|**ArrayType**|vector or list|list(type="array", elementType=*elementType*, containsNull=[*containsNull*])<br/>**Note:** The default value of *containsNull* is TRUE.|
|**MapType**|environment|list(type="map", keyType=*keyType*, valueType=*valueType*, valueContainsNull=[*valueContainsNull*])<br/> **Note:** The default value of *valueContainsNull* is TRUE.|
|**StructType**|named list|list(type="struct", fields=*fields*)<br/> **Note:** *fields* is a Seq of StructFields. Also, two fields with the same name are not allowed.|
|**StructField**|The value type in R of the data type of this field (For example, integer for a StructField with the data type IntegerType)|list(name=*name*, type=*dataType*, nullable=[*nullable*])<br/> **Note:** The default value of *nullable* is TRUE.|

</div>

<div data-lang="SQL"  markdown="1">

The following table shows the type names as well as aliases used in Spark SQL parser for each data type.

|Data type|SQL name|
|---------|--------|
|**BooleanType**|BOOLEAN|
|**ByteType**|BYTE, TINYINT|
|**ShortType**|SHORT, SMALLINT|
|**IntegerType**|INT, INTEGER|
|**LongType**|LONG, BIGINT|
|**FloatType**|FLOAT, REAL|
|**DoubleType**|DOUBLE|
|**DateType**|DATE|
|**TimestampType**|TIMESTAMP|
|**StringType**|STRING|
|**BinaryType**|BINARY|
|**DecimalType**|DECIMAL, DEC, NUMERIC|
|**YearMonthIntervalType**|INTERVAL YEAR, INTERVAL YEAR TO MONTH, INTERVAL MONTH|
|**DayTimeIntervalType**|INTERVAL DAY, INTERVAL DAY TO HOUR, INTERVAL DAY TO MINUTE, INTERVAL DAY TO SECOND, INTERVAL HOUR, INTERVAL HOUR TO MINUTE, INTERVAL HOUR TO SECOND, INTERVAL MINUTE, INTERVAL MINUTE TO SECOND, INTERVAL SECOND|
|**ArrayType**|ARRAY\<element_type>|
|**StructType**|STRUCT<field1_name: field1_type, field2_name: field2_type, ...><br/> **Note:** ':' is optional.|
|**MapType**|MAP<key_type, value_type>|

</div>
</div>

### Floating Point Special Values

Spark SQL supports several special floating point values in a case-insensitive manner:

 * Inf/+Inf/Infinity/+Infinity: positive infinity
   * ```FloatType```: equivalent to Scala <code>Float.PositiveInfinity</code>.
   * ```DoubleType```: equivalent to Scala <code>Double.PositiveInfinity</code>.
 * -Inf/-Infinity: negative infinity
   * ```FloatType```: equivalent to Scala <code>Float.NegativeInfinity</code>.
   * ```DoubleType```: equivalent to Scala <code>Double.NegativeInfinity</code>.
 * NaN: not a number
   * ```FloatType```: equivalent to Scala <code>Float.NaN</code>.
   * ```DoubleType```:  equivalent to Scala <code>Double.NaN</code>.

#### Positive/Negative Infinity Semantics

There is special handling for positive and negative infinity. They have the following semantics:

 * Positive infinity multiplied by any positive value returns positive infinity.
 * Negative infinity multiplied by any positive value returns negative infinity.
 * Positive infinity multiplied by any negative value returns negative infinity.
 * Negative infinity multiplied by any negative value returns positive infinity.
 * Positive/negative infinity multiplied by 0 returns NaN.
 * Positive/negative infinity is equal to itself.
 * In aggregations, all positive infinity values are grouped together. Similarly, all negative infinity values are grouped together.
 * Positive infinity and negative infinity are treated as normal values in join keys.
 * Positive infinity sorts lower than NaN and higher than any other values.
 * Negative infinity sorts lower than any other values.

#### NaN Semantics

There is special handling for not-a-number (NaN) when dealing with `float` or `double` types that
do not exactly match standard floating point semantics.
Specifically:

 * NaN = NaN returns true.
 * In aggregations, all NaN values are grouped together.
 * NaN is treated as a normal value in join keys.
 * NaN values go last when in ascending order, larger than any other numeric value.

#### Examples

```sql
SELECT double('infinity') AS col;
+--------+
|     col|
+--------+
|Infinity|
+--------+

SELECT float('-inf') AS col;
+---------+
|      col|
+---------+
|-Infinity|
+---------+

SELECT float('NaN') AS col;
+---+
|col|
+---+
|NaN|
+---+

SELECT double('infinity') * 0 AS col;
+---+
|col|
+---+
|NaN|
+---+

SELECT double('-infinity') * (-1234567) AS col;
+--------+
|     col|
+--------+
|Infinity|
+--------+

SELECT double('infinity') < double('NaN') AS col;
+----+
| col|
+----+
|true|
+----+

SELECT double('NaN') = double('NaN') AS col;
+----+
| col|
+----+
|true|
+----+

SELECT double('inf') = double('infinity') AS col;
+----+
| col|
+----+
|true|
+----+

CREATE TABLE test (c1 int, c2 double);
INSERT INTO test VALUES (1, double('infinity'));
INSERT INTO test VALUES (2, double('infinity'));
INSERT INTO test VALUES (3, double('inf'));
INSERT INTO test VALUES (4, double('-inf'));
INSERT INTO test VALUES (5, double('NaN'));
INSERT INTO test VALUES (6, double('NaN'));
INSERT INTO test VALUES (7, double('-infinity'));
SELECT COUNT(*), c2 FROM test GROUP BY c2;
+---------+---------+
| count(1)|       c2|
+---------+---------+
|        2|      NaN|
|        2|-Infinity|
|        3| Infinity|
+---------+---------+
```
