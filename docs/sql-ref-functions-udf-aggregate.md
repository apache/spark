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

### Aggregator[-IN, BUF, OUT]

A base class for user-defined aggregations, which can be used in Dataset operations to take all of the elements of a group and reduce them to a single value.

  ***IN*** - The input type for the aggregation.

  ***BUF*** - The type of the intermediate value of the reduction.

  ***OUT*** - The type of the final output result.

* **bufferEncoder: Encoder[BUF]**

    Specifies the Encoder for the intermediate value type.

* **finish(reduction: BUF): OUT**

    Transform the output of the reduction.

* **merge(b1: BUF, b2: BUF): BUF**

    Merge two intermediate values.

* **outputEncoder: Encoder[OUT]**

    Specifies the Encoder for the final output value type.

* **reduce(b: BUF, a: IN): BUF**

     Aggregate input value `a` into current intermediate value. For performance, the function may modify `b` and return it instead of constructing new object for `b`.

* **zero: BUF**

    The initial value of the intermediate result for this aggregation.

### Examples

#### Type-Safe User-Defined Aggregate Functions

User-defined aggregations for strongly typed Datasets revolve around the [Aggregator](api/scala/org/apache/spark/sql/expressions/Aggregator.html) abstract class.
For example, a type-safe user-defined average can look like:
<div class="codetabs">
<div data-lang="scala"  markdown="1">
  {% include_example typed_custom_aggregation scala/org/apache/spark/examples/sql/UserDefinedTypedAggregation.scala%}
</div>
<div data-lang="java"  markdown="1">
  {% include_example typed_custom_aggregation java/org/apache/spark/examples/sql/JavaUserDefinedTypedAggregation.java%}
</div>
</div>

#### Untyped User-Defined Aggregate Functions

Typed aggregations, as described above, may also be registered as untyped aggregating UDFs for use with DataFrames.
For example, a user-defined average for untyped DataFrames can look like:
<div class="codetabs">
<div data-lang="scala"  markdown="1">
  {% include_example untyped_custom_aggregation scala/org/apache/spark/examples/sql/UserDefinedUntypedAggregation.scala%}
</div>
<div data-lang="java"  markdown="1">
  {% include_example untyped_custom_aggregation java/org/apache/spark/examples/sql/JavaUserDefinedUntypedAggregation.java%}
</div>
<div data-lang="SQL"  markdown="1">
```sql
-- Compile and place UDAF MyAverage in a JAR file called `MyAverage.jar` in /tmp.
CREATE FUNCTION myAverage AS 'MyAverage' USING JAR '/tmp/MyAverage.jar';

SHOW USER FUNCTIONS;
+------------------+
|          function|
+------------------+
| default.myAverage|
+------------------+

CREATE TEMPORARY VIEW employees
USING org.apache.spark.sql.json
OPTIONS (
    path "examples/src/main/resources/employees.json"
);

SELECT * FROM employees;
+-------+------+
|   name|salary|
+-------+------+
|Michael|  3000|
|   Andy|  4500|
| Justin|  3500|
|  Berta|  4000|
+-------+------+

SELECT myAverage(salary) as average_salary FROM employees;
+--------------+
|average_salary|
+--------------+
|        3750.0|
+--------------+
```
</div>
</div>

### Related Statements

* [Scalar User Defined Functions (UDFs)](sql-ref-functions-udf-scalar.html)
* [Integration with Hive UDFs/UDAFs/UDTFs](sql-ref-functions-udf-hive.html)
