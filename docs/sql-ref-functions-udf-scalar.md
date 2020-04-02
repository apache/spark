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

User-Defined Functions (UDFs) are user-programmable routines that act on one row. This documentation lists the classes that are required for creating and registering UDFs. It also contains examples that demonstrate how to define and register UDFs and invoke them in Spark SQL.


### UserDefinedFunction

A user-defined function. To create one, use the `udf` functions in `functions`.

<dl>
  <dt><code><em>asNonNullable(): UserDefinedFunction</em></code></dt>
  <dd>
    Updates UserDefinedFunction to non-nullable.
  </dd>
</dl>

<dl>
  <dt><code><em>asNondeterministic(): UserDefinedFunction</em></code></dt>
  <dd>
    Updates UserDefinedFunction to nondeterministic.
  </dd>
</dl>

<dl>
  <dt><code><em>deterministic: Boolean</em></code></dt>
  <dd>
    Returns true iff the UDF is deterministic, i.e. the UDF produces the same output given the same input.
  </dd>
</dl>

<dl>
  <dt><code><em>nullable: Boolean</em></code></dt>
  <dd>
    Returns true when the UDF can return a nullable value.
  </dd>
</dl>

<dl>
  <dt><code><em>withName(name: String): UserDefinedFunction</em></code></dt>
  <dd>
    Updates UserDefinedFunction with a given name.
  </dd>
</dl>

### UDFRegistration

Functions for registering user-defined functions. Use `SparkSession.udf` to access this: `spark.udf`

<dl>
  <dt><code><em>register(name: String, udf: UserDefinedFunction): UserDefinedFunction</em></code></dt>
  <dd>
    Registers a user-defined function (UDF).
  </dd>
</dl>

### Examples

{% highlight sql %}

// Define and register a zero-argument non-deterministic UDF
// UDF is deterministic by default, i.e. produces the same result for the same input.
// Scala
import org.apache.spark.sql.functions.udf

val foo = udf(() => Math.random())
spark.udf.register("random", foo.asNondeterministic())

sql("SELECT random()").show()

+------------------+
|UDF()             |
+------------------+
|0.9199799737037972|
+------------------+

// Define and register a one-argument UDF
// Scala
import org.apache.spark.sql.functions.udf

val plusOne = udf((x: Int) => x + 1)
spark.udf.register("plusOne", plusOne)

sql("SELECT plusOne(5)").show()

+------+
|UDF(5)|
+------+
|     6|
+------+

// Define a two-argument UDF and register it with Spark in one step
// Scala
import import org.apache.spark.sql.functions.udf

spark.udf.register("strLenScala", (_: String).length + (_: Int))

sql("SELECT strLenScala('test', 1))").show()

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

sql("SELECT * FROM test WHERE oneArgFilter(id)").show()

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

sql("SELECT SUM(value) FROM groupData GROUP BY groupFunction(value)").show()

+----------+
|sum(value)|
+----------+
|13        |
|300       |
+----------+

# Define and register a UDF using Python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark.udf.register("twoArgs", lambda x, y: len(x) + y, IntegerType())

spark.sql("SELECT twoArgs('test', 1)").show()

+----------------+
|twoArgs(test, 1)|
+----------------+
|               5|
+----------------+

{% endhighlight %}
