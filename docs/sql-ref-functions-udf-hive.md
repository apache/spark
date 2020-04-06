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


<pre><code>
// Register a Hive UDF and use it in Spark SQL
// Scala
// include the JAR file containing mytest.hiveUDF implementation
spark.sql("ADD JAR myHiveUDF.jar")
spark.sql("CREATE TEMPORARY FUNCTION testUDF AS 'mytest.hiveUDF'")
spark.sql("SELECT testUDF(value) FROM hiveUDFTestTable")

// Register a Hive UDAF and use it in Spark SQL
// Scala
// include the JAR file containing
// org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax
spark.sql("ADD JAR myHiveUDAF.jar")
spark.sql(
          """
            |CREATE TEMPORARY FUNCTION hive_max
            |AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax'
          """.stripMargin)
spark.sql("SELECT key % 2, hive_max(key) FROM t GROUP BY key % 2")

// Register a Hive UDTF and use it in Spark SQL
// Scala
// GenericUDTFCount2 outputs the number of rows seen, twice.
// The function source code can be found at:
// https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide+UDTF
// include the JAR file containing GenericUDTFCount2 implementation
spark.sql("ADD JAR myHiveUDTF.jar")
spark.sql(
          """
            |CREATE TEMPORARY FUNCTION udtf_count2
            |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
          """.stripMargin)
spark.sql("SELECT udtf_count2(a) FROM (SELECT 1 AS a)").show

+----+
|col1|
+----+
|   1|
|   1|
+----+

</code></pre>
