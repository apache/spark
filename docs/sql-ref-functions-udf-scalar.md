---
layout: global
title: Scalar User Defined Functions (UDFs)
displayTitle: Scalar User Defined Functions (UDFs)
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

User-Defined Functions (UDFs) are user-programmable routines that act on one row. This documentation contains examples that demonstrate how to define and register UDFs that act on a single row and invoke them in Spark SQL.

### Examples

{% highlight sql %}

// Define and register a zero argument non-deterministic UDF
// UDF is deterministic by default, i.e. produces the same result for the same input.
// Scala
import org.apache.spark.sql.functions.udf

val foo = udf(() => Math.random())
spark.udf.register("random", foo.asNondeterministic())

-- SQL
SELECT random();

+------------------+
|UDF()             |
+------------------+
|0.9199799737037972|
+------------------+

// Define and register a one argument UDF
// Scala
import org.apache.spark.sql.functions.udf

val plusOne = udf((x: Int) => x + 1)
spark.udf.register("plusOne", plusOne)

-- SQL
SELECT plusOne(5);

+------+
|UDF(5)|
+------+
|     6|
+------+

// Define a two arguments UDF and register it with Spark in one step
// Scala
import import org.apache.spark.sql.functions.udf

spark.udf.register("strLenScala", (_: String).length + (_: Int))

-- SQL
SELECT strLenScala('test', 1));

+--------------------+
|strLenScala(test, 1)|
+--------------------+
|                   5|
+--------------------+

// UDF in a WHERE clause
// Scala
import org.apache.spark.sql.functions.udf

spark.udf.register("oneArgFilter", (n: Int) => { n > 5 })
spark.range(1, 10).createOrReplaceTempView("test")

-- SQL
SELECT * FROM test WHERE oneArgFilter(id);

+---+
|id |
+---+
|6  |
|7  |
|8  |
|9  |
+---+

// UDF in a GROUP BY clause
// Scala
import org.apache.spark.sql.functions.udf

spark.udf.register("groupFunction", (n: Int) => { n > 10 })

val df = Seq(("red", 1),
             ("red", 2),
             ("blue", 10),
             ("green", 100),
             ("green", 200))
             .toDF("color", "value")
df.createOrReplaceTempView("groupData")

-- SQL
SELECT SUM(value) FROM groupData GROUP BY groupFunction(value);

+----------+
|sum(value)|
+----------+
|13        |
|300       |
+----------+

# Define and register a UDF using Python
from pyspark.sql.functions import UserDefinedFunction, udf
self.spark.catalog.registerFunction("twoArgs", lambda x, y: len(x) + y, IntegerType())

-- SQL
SELECT twoArgs('test', 1);

+----------------+
|twoArgs(test, 1)|
+----------------+
|               5|
+----------------+

{% endhighlight %}

