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

User-Defined Aggregate Functions (UDAFs) are user-programmable routines that act on multiple rows at once and return a single aggregated value as a result. This documentation contains examples that demonstrate how to define and register UDAFs in Scala and invoke them in Spark SQL.

### Examples

{% highlight sql %}

// Define and register a UDAF to calculate the sum of product of two columns
// Scala
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.udf

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
