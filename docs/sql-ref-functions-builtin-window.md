---
layout: global
title: Window Functions
displayTitle: Window Functions
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

Similarly to aggregate functions, window functions operate on a group of rows. However, unlike aggregate functions, window functions perform aggregation without reducing, calculating a return value for each row in the group. Window functions are useful for processing tasks such as calculating a moving average, computing a cumulative, or accessing the value of rows given the relative position of the current row. Spark SQL supports three types of window functions:
 * Ranking Functions
 * Analytic Functions
 * Aggregate Functions

### Types of Window Functions

#### Ranking Functions

<table class="table">
  <thead>
    <tr><th style="width:25%">Function</th><th>Description</th></tr>
  </thead>
    <tr>
      <td><b> rank </b></td>
      <td>Returns the rank of rows within a window partition. This is equivalent to the RANK function in SQL. </td>
    </tr>
    <tr>
      <td><b> dense_rank </b></td>
      <td>Returns the rank of rows within a window partition, without any gaps. This is equivalent to the DENSE_RANK function in SQL.</td>
    </tr>
    <tr>
      <td><b> percent_rank </b></td>
      <td>Returns the relative rank (i.e. percentile) of rows within a window partition. This is equivalent to the PERCENT_RANK function in SQL.</td>
    </tr>
    <tr>
      <td><b> ntile </b></td>
      <td>Returns the ntile group id (from 1 to `n` inclusive) in an ordered window partition. This is equivalent to the NTILE function in SQL.</td>
    </tr>
    <tr>
      <td><b> row_number </b></td>
      <td>Returns a sequential number starting from 1 within a window partition. This is equivalent to the ROWNUMBER function in SQL.</td>
    </tr>
</table>

#### Analytic Functions

<table class="table">
  <thead>
    <tr><th style="width:25%">Function</th><th>Description</th></tr>
  </thead>
    <tr>
      <td><b> cume_dist </b></td>
      <td>Returns the cumulative distribution of values within a window partition, i.e. the fraction of rows that are below the current row. This is equivalent to the CUMEDIST function in SQL. </td>
    </tr>
    <tr>
      <td><b> lag </b></td>
      <td>Returns the value that is <code>offset</code> rows before the current row, and <code>null</code> if there are less than <code>offset</code> rows before the current row.</td>
    </tr>
    <tr>
      <td><b> lead </b></td>
      <td>Returns the value that is <code>offset</code> rows after the current row, and <code>null</code> if there are less than <code>offset</code> rows after the current row. This is equivalent to the LEAD function in SQL.</td>
    </tr>
</table>

#### Aggregate Functions

Any of the aggregate functions can be used as a window function. Please refer to the complete list of Spark [Aggregate Functions](sql-ref-functions-builtin-aggregate.html).

### How to Use Window Functions

  * Mark a function as window function by using `over`.
    - SQL: Add an OVER clause after the window function, e.g. avg (...) OVER (...);
    - DataFrame API: Call the window function's `over` method, e.g. rank().over(...)
  * Define the window specification associated with this function. A window specification includes partitioning specification, ordering specification, and frame specification.
    - Partitioning Specification:
      - SQL: PARTITION BY
      - DataFrame API: Window.partitionBy (...)
    - Ordering Specification:
      - SQL: Order BY
      - DataFrame API: Window.orderBy (...)
    - Frame Specification:
      - SQL: ROWS (for ROW frame), RANGE (for RANGE frame)
      - DataFrame API: WindowSpec.rowsBetween (for ROW frame), WindowSpec.rangeBetween (for RANGE frame)

### Examples

{% highlight scala %}

  import spark.implicits._

  val data = Seq(("Lisa", "Sales", 10000),
    ("Evan", "Sales", 32000),
    ("Fred", "Engineering", 21000),
    ("Helen", "Marketing", 29000),
    ("Alex", "Sales", 30000),
    ("Tom", "Engineering", 23000),
    ("Jane", "Marketing", 29000),
    ("Jeff", "Marketing", 35000),
    ("Paul", "Engineering", 29000),
    ("Chloe", "Engineering", 23000)
  )
  val df = data.toDF("name", "dept", "salary")
  df.show()
  // +-----+-----------+------+
  // | name|       dept|salary|
  // +-----+-----------+------+
  // | Lisa|      Sales| 10000|
  // | Evan|      Sales| 32000|
  // | Fred|Engineering| 21000|
  // |Helen|  Marketing| 29000|
  // | Alex|      Sales| 30000|
  // |  Tom|Engineering| 23000|
  // | Jane|  Marketing| 29000|
  // | Jeff|  Marketing| 35000|
  // | Paul|Engineering| 29000|
  // |Chloe|Engineering| 23000|
  // +-----+-----------+------+

  val windowSpec = Window.partitionBy("dept").orderBy("salary")
  windowSpec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

  // Using Ranking Functions
  df.withColumn("rank", rank().over(windowSpec)).show()
  // +-----+-----------+------+----+
  // | name|       dept|salary|rank|
  // +-----+-----------+------+----+
  // |Helen|  Marketing| 29000|   1|
  // | Jane|  Marketing| 29000|   1|
  // | Jeff|  Marketing| 35000|   3|
  // | Fred|Engineering| 21000|   1|
  // |  Tom|Engineering| 23000|   2|
  // |Chloe|Engineering| 23000|   2|
  // | Paul|Engineering| 29000|   4|
  // | Lisa|      Sales| 10000|   1|
  // | Alex|      Sales| 30000|   2|
  // | Evan|      Sales| 32000|   3|
  // +-----+-----------+------+----+

  df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()
  // +-----+-----------+------+----------+
  // | name|       dept|salary|dense_rank|
  // +-----+-----------+------+----------+
  // |Helen|  Marketing| 29000|         1|
  // | Jane|  Marketing| 29000|         1|
  // | Jeff|  Marketing| 35000|         2|
  // | Fred|Engineering| 21000|         1|
  // |  Tom|Engineering| 23000|         2|
  // |Chloe|Engineering| 23000|         2|
  // | Paul|Engineering| 29000|         3|
  // | Lisa|      Sales| 10000|         1|
  // | Alex|      Sales| 30000|         2|
  // | Evan|      Sales| 32000|         3|
  // +-----+-----------+------+----------+

  // Using Analytic Functions
  df.withColumn("cume_dist", cume_dist().over(windowSpec)).show()
  // +-----+-----------+------+------------------+
  // | name|       dept|salary|         cume_dist|
  // +-----+-----------+------+------------------+
  // |Helen|  Marketing| 29000|0.6666666666666666|
  // | Jane|  Marketing| 29000|0.6666666666666666|
  // | Jeff|  Marketing| 35000|               1.0|
  // | Fred|Engineering| 21000|              0.25|
  // |  Tom|Engineering| 23000|              0.75|
  // |Chloe|Engineering| 23000|              0.75|
  // | Paul|Engineering| 29000|               1.0|
  // | Lisa|      Sales| 10000|0.3333333333333333|
  // | Alex|      Sales| 30000|0.6666666666666666|
  // | Evan|      Sales| 32000|               1.0|
  // +-----+-----------+------+------------------+

  df.withColumn("lag", lag("salary", 2).over(windowSpec)).show()
  // +-----+-----------+------+-----+
  // |Helen|  Marketing| 29000| null|
  // | Jane|  Marketing| 29000| null|
  // | Jeff|  Marketing| 35000|29000|
  // | Fred|Engineering| 21000| null|
  // |  Tom|Engineering| 23000| null|
  // |Chloe|Engineering| 23000|21000|
  // | Paul|Engineering| 29000|23000|
  // | Lisa|      Sales| 10000| null|
  // | Alex|      Sales| 30000| null|
  // | Evan|      Sales| 32000|10000|
  // +-----+-----------+------+-----+

  // Using Aggregate Functions
  df.withColumn("min", min(col("salary")).over(windowSpec)).show()
  // +-----+-----------+------+-----+
  // | name|       dept|salary|  min|
  // +-----+-----------+------+-----+
  // |Helen|  Marketing| 29000|29000|
  // | Jane|  Marketing| 29000|29000|
  // | Jeff|  Marketing| 35000|29000|
  // | Fred|Engineering| 21000|21000|
  // |  Tom|Engineering| 23000|21000|
  // |Chloe|Engineering| 23000|21000|
  // | Paul|Engineering| 29000|21000|
  // | Lisa|      Sales| 10000|10000|
  // | Alex|      Sales| 30000|10000|
  // | Evan|      Sales| 32000|10000|
  // +-----+-----------+------+-----+

{% endhighlight %}
