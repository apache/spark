---
layout: global
title: User Defined Aggregate Functions (UDAFs)
displayTitle: User Defined Aggregate Functions (UDAFs)
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

User-Defined Aggregate Functions (UDAFs) are user-programmable routines that act on multiple rows at once and return a single aggregated value as a result. This documentation lists the classes that are required for creating and registering UDAFs. It also contains examples that demonstrate how to define and register UDAFs in Scala and invoke them in Spark SQL.

### org.apache.spark.sql.expressions.Aggregator[-IN, BUF, OUT]

A base class for user-defined aggregations, which can be used in Dataset operations to take all of the elements of a group and reduce them to a single value.
- IN The input type for the aggregation.

- BUF The type of the intermediate value of the reduction.

- OUT The type of the final output result.

<dl>
  <dt><code><em>bufferEncoder: Encoder[BUF]</em></code></dt>
  <dd>
    Register a deterministic Java UDF(0-22) instance as user-defined function (UDF).
  </dd>
</dl>


<dl>
  <dt><code><em>finish(reduction: BUF): OUT</em></code></dt>
  <dd>
    Transform the output of the reduction.
  </dd>
</dl>

<dl>
  <dt><code><em>merge(b1: BUF, b2: BUF): BUF</em></code></dt>
  <dd>
    Merge two intermediate values.
  </dd>
</dl>

<dl>
  <dt><code><em>outputEncoder: Encoder[OUT]</em></code></dt>
  <dd>
    Specifies the Encoder for the final output value type.
  </dd>
</dl>

<dl>
  <dt><code><em>reduce(b: BUF, a: IN): BUF</em></code></dt>
  <dd>
    Combine two values to produce a new value. For performance, the function may modify b and return it instead of constructing new object for b.
  </dd>
</dl>

<dl>
  <dt><code><em>zero: BUF</em></code></dt>
  <dd>
    A zero value for this aggregation.
  </dd>
</dl>

### org.apache.spark.sql.UDFRegistration

Functions for registering user-defined functions. Use `SparkSession.udf` to access this: `spark.udf`

<dl>
  <dt><code><em>register(name: String, udf: UserDefinedFunction): UserDefinedFunction</em></code></dt>
  <dd>
    Registers a user-defined function (UDF).
  </dd>
</dl>

### Examples

{% highlight sql %}

// Define and register a UDAF to calculate the sum of product of two columns
// Scala
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.udaf

val agg = udaf(new Aggregator[(Long, Long), Long, Long] {
  def zero: Long = 0L
  def reduce(b: Long, a: (Long, Long)): Long = b + (a._1 * a._2)
  def merge(b1: Long, b2: Long): Long = b1 + b2
  def finish(r: Long): Long = r
  def bufferEncoder: Encoder[Long] = Encoders.scalaLong
  def outputEncoder: Encoder[Long] = Encoders.scalaLong
})

spark.udf.register("agg", agg)

val df = Seq(
  (1, 1),
  (1, 5),
  (2, 10),
  (2, -1),
  (4, 7),
  (3, 8),
  (2, 4))
  .toDF("a", "b")

df.createOrReplaceTempView("testUDAF")

-- SQL
SELECT agg(a, b) FROM testUDAF;

+---------------------------------------------+
|$anon$2(CAST(a AS BIGINT), CAST(b AS BIGINT))|
+---------------------------------------------+
|84                                           |
+---------------------------------------------+

{% endhighlight %}
