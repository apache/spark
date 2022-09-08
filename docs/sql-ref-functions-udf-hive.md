---
layout: global
title: Integration with Hive UDFs/UDAFs/UDTFs
displayTitle: Integration with Hive UDFs/UDAFs/UDTFs
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

### Description

Spark SQL supports integration of Hive UDFs, UDAFs and UDTFs. Similar to Spark UDFs and UDAFs, Hive UDFs work on a single row as input and generate a single row as output, while Hive UDAFs operate on multiple rows and return a single aggregated row as a result. In addition, Hive also supports UDTFs (User Defined Tabular Functions) that act on one row as input and return multiple rows as output. To use Hive UDFs/UDAFs/UTFs, the user should register them in Spark, and then use them in Spark SQL queries.

### Examples

Hive has two UDF interfaces: [UDF](https://github.com/apache/hive/blob/master/udf/src/java/org/apache/hadoop/hive/ql/exec/UDF.java) and [GenericUDF](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDF.java).
An example below uses [GenericUDFAbs](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFAbs.java) derived from `GenericUDF`.

```sql
-- Register `GenericUDFAbs` and use it in Spark SQL.
-- Note that, if you use your own programmed one, you need to add a JAR containing it
-- into a classpath,
-- e.g., ADD JAR yourHiveUDF.jar;
CREATE TEMPORARY FUNCTION testUDF AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs';

SELECT * FROM t;
+-----+
|value|
+-----+
| -1.0|
|  2.0|
| -3.0|
+-----+

SELECT testUDF(value) FROM t;
+--------------+
|testUDF(value)|
+--------------+
|           1.0|
|           2.0|
|           3.0|
+--------------+
```


An example below uses [GenericUDTFExplode](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDTFExplode.java) derived from [GenericUDTF](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDTF.java).

```sql
-- Register `GenericUDTFExplode` and use it in Spark SQL
CREATE TEMPORARY FUNCTION hiveUDTF
    AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode';

SELECT * FROM t;
+------+
| value|
+------+
|[1, 2]|
|[3, 4]|
+------+

SELECT hiveUDTF(value) FROM t;
+---+
|col|
+---+
|  1|
|  2|
|  3|
|  4|
+---+
```

Hive has two UDAF interfaces: [UDAF](https://github.com/apache/hive/blob/master/udf/src/java/org/apache/hadoop/hive/ql/exec/UDAF.java) and [GenericUDAFResolver](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDAFResolver.java).
An example below uses [GenericUDAFSum](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDAFSum.java) derived from `GenericUDAFResolver`.

```sql
-- Register `GenericUDAFSum` and use it in Spark SQL
CREATE TEMPORARY FUNCTION hiveUDAF
    AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum';

SELECT * FROM t;
+---+-----+
|key|value|
+---+-----+
|  a|    1|
|  a|    2|
|  b|    3|
+---+-----+

SELECT key, hiveUDAF(value) FROM t GROUP BY key;
+---+---------------+
|key|hiveUDAF(value)|
+---+---------------+
|  b|              3|
|  a|              3|
+---+---------------+
```