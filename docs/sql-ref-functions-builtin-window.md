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

### How to Use Window Functions

  * Mark a function as window function by using `over`.
    - SQL: Add an OVER clause after the window function, e.g. avg ( ... ) OVER ( ... );
    - DataFrame API: Call the window function's `over` method, e.g. rank ( ).over ( ... )
  * Define the window specification associated with this function. A window specification includes partitioning specification, ordering specification, and frame specification.
    - Partitioning Specification:
      - SQL: PARTITION BY
      - DataFrame API: Window.partitionBy ( ... )
    - Ordering Specification:
      - SQL: Order BY
      - DataFrame API: Window.orderBy ( ... )
    - Frame Specification:
      - SQL: ROWS ( for ROW frame ), RANGE ( for RANGE frame )
      - DataFrame API: WindowSpec.rowsBetween ( for ROW frame ), WindowSpec.rangeBetween ( for RANGE frame )

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
    +-----+-----------+------+
    | name|       dept|salary|
    +-----+-----------+------+
    | Lisa|      Sales| 10000|
    | Evan|      Sales| 32000|
    | Fred|Engineering| 21000|
    |Helen|  Marketing| 29000|
    | Alex|      Sales| 30000|
    |  Tom|Engineering| 23000|
    | Jane|  Marketing| 29000|
    | Jeff|  Marketing| 35000|
    | Paul|Engineering| 29000|
    |Chloe|Engineering| 23000|
    +-----+-----------+------+

  val windowSpec = Window.partitionBy("dept").orderBy("salary")
  windowSpec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

  // Using Ranking Functions
  df.withColumn("rank", rank().over(windowSpec)).show()
    +-----+-----------+------+----+
    | name|       dept|salary|rank|
    +-----+-----------+------+----+
    |Helen|  Marketing| 29000|   1|
    | Jane|  Marketing| 29000|   1|
    | Jeff|  Marketing| 35000|   3|
    | Fred|Engineering| 21000|   1|
    |  Tom|Engineering| 23000|   2|
    |Chloe|Engineering| 23000|   2|
    | Paul|Engineering| 29000|   4|
    | Lisa|      Sales| 10000|   1|
    | Alex|      Sales| 30000|   2|
    | Evan|      Sales| 32000|   3|
    +-----+-----------+------+----+

  df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()
    +-----+-----------+------+----------+
    | name|       dept|salary|dense_rank|
    +-----+-----------+------+----------+
    |Helen|  Marketing| 29000|         1|
    | Jane|  Marketing| 29000|         1|
    | Jeff|  Marketing| 35000|         2|
    | Fred|Engineering| 21000|         1|
    |  Tom|Engineering| 23000|         2|
    |Chloe|Engineering| 23000|         2|
    | Paul|Engineering| 29000|         3|
    | Lisa|      Sales| 10000|         1|
    | Alex|      Sales| 30000|         2|
    | Evan|      Sales| 32000|         3|
    +-----+-----------+------+----------+

  // Using Analytic Functions
  df.withColumn("cume_dist", cume_dist().over(windowSpec)).show()
    +-----+-----------+------+------------------+
    | name|       dept|salary|         cume_dist|
    +-----+-----------+------+------------------+
    |Helen|  Marketing| 29000|0.6666666666666666|
    | Jane|  Marketing| 29000|0.6666666666666666|
    | Jeff|  Marketing| 35000|               1.0|
    | Fred|Engineering| 21000|              0.25|
    |  Tom|Engineering| 23000|              0.75|
    |Chloe|Engineering| 23000|              0.75|
    | Paul|Engineering| 29000|               1.0|
    | Lisa|      Sales| 10000|0.3333333333333333|
    | Alex|      Sales| 30000|0.6666666666666666|
    | Evan|      Sales| 32000|               1.0|
    +-----+-----------+------+------------------+

  df.withColumn("lag", lag("salary", 2).over(windowSpec)).show()
    +-----+-----------+------+-----+
    |Helen|  Marketing| 29000| null|
    | Jane|  Marketing| 29000| null|
    | Jeff|  Marketing| 35000|29000|
    | Fred|Engineering| 21000| null|
    |  Tom|Engineering| 23000| null|
    |Chloe|Engineering| 23000|21000|
    | Paul|Engineering| 29000|23000|
    | Lisa|      Sales| 10000| null|
    | Alex|      Sales| 30000| null|
    | Evan|      Sales| 32000|10000|
    +-----+-----------+------+-----+

  // Using Aggregate Functions
  df.withColumn("min", min(col("salary")).over(windowSpec)).show()
    +-----+-----------+------+-----+
    | name|       dept|salary|  min|
    +-----+-----------+------+-----+
    |Helen|  Marketing| 29000|29000|
    | Jane|  Marketing| 29000|29000|
    | Jeff|  Marketing| 35000|29000|
    | Fred|Engineering| 21000|21000|
    |  Tom|Engineering| 23000|21000|
    |Chloe|Engineering| 23000|21000|
    | Paul|Engineering| 29000|21000|
    | Lisa|      Sales| 10000|10000|
    | Alex|      Sales| 30000|10000|
    | Evan|      Sales| 32000|10000|
    +-----+-----------+------+-----+

{% endhighlight %}
