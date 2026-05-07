---
layout: global
title: Parameter markers
displayTitle: Parameter markers
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

Parameter markers are [named](#named-parameter-markers) or [unnamed](#unnamed-parameter-markers) typed placeholder variables used to supply values from the API invoking the SQL statement.

Using parameter markers protects your code from SQL injection attacks since it clearly separates provided values from the SQL statements.

You cannot mix named and unnamed parameter markers in the same SQL statement.

Parameter markers can be used wherever literal values are allowed in the SQL syntax.

To parameterize identifiers (such as table or column names), use the [`IDENTIFIER` clause](sql-ref-identifier-clause.html).

Parameter markers can be provided by:

- **Python** using its [pyspark.sql.SparkSession.sql()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html) API.
- **Scala** using its [org.apache.spark.sql.SparkSession.sql()](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SparkSession.html) API.
- **Java** using its [org.apache.spark.sql.SparkSession.sql()](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.html) API.
- **SQL** using the [`EXECUTE IMMEDIATE` statement](sql-ref-syntax-aux-exec-imm.html).

#### Named parameter markers

Named parameter markers are typed placeholder variables. The API invoking the SQL statement must supply name-value pairs to associate each parameter marker with a value.

##### Syntax

```
 :parameter_name
```

##### Parameters

- **named_parameter_name**

  A reference to a supplied parameter marker in form of an unqualified identifier.

##### Notes

You can reference the same parameter marker multiple times within the same SQL Statement.
If no value has been bound to the parameter marker an `UNBOUND_SQL_PARAMETER` error is raised.
You are not required to reference all supplied parameter markers.

The mandatory preceding `:` (colon) differentiates the namespace of named parameter markers from that of column names and SQL parameters.

##### Examples

The following example defines two parameter markers:

- **later**: An `INTERVAL HOUR` with value 3.
- **x**: A `DOUBLE` with value 15.0

`x` is referenced multiple times, while `later` is referenced once.

- SQL

  ```sql
  > DECLARE stmtStr = 'SELECT current_timestamp() + :later, :x * :x AS square';
  > EXECUTE IMMEDIATE stmtStr USING INTERVAL '3' HOURS AS later, 15.0 AS x;
    2024-01-19 16:17:16.692303  225.00
  ```

- Scala

  ```scala
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark named parameter marker example")
    .getOrCreate()

  val argMap = Map("later" -> java.time.Duration.ofHours(3), "x" -> 15.0)
  spark.sql(
    sqlText = "SELECT current_timestamp() + :later, :x * :x AS square",
    args = argMap).show()
  // +----------------------------------------+------+
  // |current_timestamp() + INTERVAL '03' HOUR|square|
  // +----------------------------------------+------+
  // |                    2023-02-27 17:48:...|225.00|
  // +----------------------------------------+------+
  ```
- Java

  ```java
  import org.apache.spark.sql.*;
  import static java.util.Map.entry;

  SparkSession spark = SparkSession
    .builder()
    .appName("Java Spark named parameter marker example")
      .getOrCreate();

  Map<String, String> argMap = Map.ofEntries(
    entry("later", java.time.Duration.ofHours(3)),
    entry("x", 15.0)
  );

  spark.sql(
    sqlText = "SELECT current_timestamp() + :later, :x * :x AS square",
    args = argMap).show();
  // +----------------------------------------+------+
  // |current_timestamp() + INTERVAL '03' HOUR|square|
  // +----------------------------------------+------+
  // |                    2023-02-27 17:48:...|225.00|
  // +----------------------------------------+------+
  ```

- Python

  ```python
  spark.sql("SELECT :x * :y * :z AS volume", args = { "x" : 3, "y" : 4, "z" : 5 }).show()
  // +------+
  // |volume|
  // +------+
  // |    60|
  // +------+
  ```

#### Unnamed parameter markers

Unnamed parameter markers are typed placeholder variables. The API invoking the SQL statement must supply an array of arguments to associate each parameter marker with a value in the order in which they appear.

##### Syntax

```
 ?
```

##### Parameters

- **`?`**: A reference to a supplied parameter marker in form of a question mark.

##### Notes

Each occurrence of an unnamed parameter marker consumes a value provided by the API invoking the SQL statement in order.
If no value has been bound to the parameter marker, an `UNBOUND_SQL_PARAMETER` error is raised.
You are not required to consume all provided values.

### Examples

The following example defines three parameter markers:

- An `INTERVAL HOUR` with value 3.
- Two `DOUBLE` with value 15.0 each.

Since the parameters are unnamed each provided value is consumed by at most one parameter.

- SQL

  ```sql
  DECLARE stmtStr = 'SELECT current_timestamp() + ?, ? * ? AS square';
  EXECUTE IMMEDIATE stmtStr USING INTERVAL '3' HOURS, 15.0, 15.0;
    2024-01-19 16:17:16.692303  225.00
  ```

- Scala

  ```scala
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark unnamed parameter marker example")
    .getOrCreate()

  val argArr = Array(java.time.Duration.ofHours(3), 15.0, 15.0)

  spark.sql(
    sqlText = "SELECT current_timestamp() + ?, ? * ? AS square", args = argArr).show()
  // +----------------------------------------+------+
  // |current_timestamp() + INTERVAL '03' HOUR|square|
  // +----------------------------------------+------+
  // |                    2023-02-27 17:48:...|225.00|
  // +----------------------------------------+------+
  ```

- Java

  ```java
  import org.apache.spark.sql.*;

  SparkSession spark = SparkSession
    .builder()
    .appName("Java Spark unnamed parameter marker example")
    .getOrCreate();

  Object[] argArr = new Object[] { java.time.Duration.ofHours(3), 15.0, 15.0 }

  spark.sql(
    sqlText = "SELECT current_timestamp() + ?, ? * ? AS square",
    args = argArr).show();
  // +----------------------------------------+------+
  // |current_timestamp() + INTERVAL '03' HOUR|square|
  // +----------------------------------------+------+
  // |                    2023-02-27 17:48:...|225.00|
  // +----------------------------------------+------+
  ```

- Python

  ```python
  spark.sql("SELECT ? * ? * ? AS volume", args = [ 3, 4, 5 ]).show()
  // +------+
  // |volume|
  // +------+
  // |    60|
  // +------+
  ```
