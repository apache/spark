---
layout: global
title: Compatibility with Apache Hive
displayTitle: Compatibility with Apache Hive
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

* Table of contents
{:toc}

Spark SQL is designed to be compatible with the Hive Metastore, SerDes and UDFs.
Currently, Hive SerDes and UDFs are based on Hive 1.2.1,
and Spark SQL can be connected to different versions of Hive Metastore
(from 0.12.0 to 2.3.5 and 3.1.0 to 3.1.1. Also see [Interacting with Different Versions of Hive Metastore](sql-data-sources-hive-tables.html#interacting-with-different-versions-of-hive-metastore)).

#### Deploying in Existing Hive Warehouses

The Spark SQL Thrift JDBC server is designed to be "out of the box" compatible with existing Hive
installations. You do not need to modify your existing Hive Metastore or change the data placement
or partitioning of your tables.

### Supported Hive Features

Spark SQL supports the vast majority of Hive features, such as:

* Hive query statements, including:
  * `SELECT`
  * `GROUP BY`
  * `ORDER BY`
  * `CLUSTER BY`
  * `SORT BY`
* All Hive operators, including:
  * Relational operators (`=`, `⇔`, `==`, `<>`, `<`, `>`, `>=`, `<=`, etc)
  * Arithmetic operators (`+`, `-`, `*`, `/`, `%`, etc)
  * Logical operators (`AND`, `&&`, `OR`, `||`, etc)
  * Complex type constructors
  * Mathematical functions (`sign`, `ln`, `cos`, etc)
  * String functions (`instr`, `length`, `printf`, etc)
* User defined functions (UDF)
* User defined aggregation functions (UDAF)
* User defined serialization formats (SerDes)
* Window functions
* Joins
  * `JOIN`
  * `{LEFT|RIGHT|FULL} OUTER JOIN`
  * `LEFT SEMI JOIN`
  * `CROSS JOIN`
* Unions
* Sub-queries
  * `SELECT col FROM ( SELECT a + b AS col from t1) t2`
* Sampling
* Explain
* Partitioned tables including dynamic partition insertion
* View
  * If column aliases are not specified in view definition queries, both Spark and Hive will
    generate alias names, but in different ways. In order for Spark to be able to read views created
    by Hive, users should explicitly specify column aliases in view definition queries. As an
    example, Spark cannot read `v1` created as below by Hive.

    ```
    CREATE VIEW v1 AS SELECT * FROM (SELECT c + 1 FROM (SELECT 1 c) t1) t2;
    ```

    Instead, you should create `v1` as below with column aliases explicitly specified.

    ```
    CREATE VIEW v1 AS SELECT * FROM (SELECT c + 1 AS inc_c FROM (SELECT 1 c) t1) t2;
    ```

* All Hive DDL Functions, including:
  * `CREATE TABLE`
  * `CREATE TABLE AS SELECT`
  * `ALTER TABLE`
* Most Hive Data types, including:
  * `TINYINT`
  * `SMALLINT`
  * `INT`
  * `BIGINT`
  * `BOOLEAN`
  * `FLOAT`
  * `DOUBLE`
  * `STRING`
  * `BINARY`
  * `TIMESTAMP`
  * `DATE`
  * `ARRAY<>`
  * `MAP<>`
  * `STRUCT<>`

### Unsupported Hive Functionality

Below is a list of Hive features that we don't support yet. Most of these features are rarely used
in Hive deployments.

**Major Hive Features**

* Tables with buckets: bucket is the hash partitioning within a Hive table partition. Spark SQL
  doesn't support buckets yet.


**Esoteric Hive Features**

* `UNION` type
* Unique join
* Column statistics collecting: Spark SQL does not piggyback scans to collect column statistics at
  the moment and only supports populating the sizeInBytes field of the hive metastore.

**Hive Input/Output Formats**

* File format for CLI: For results showing back to the CLI, Spark SQL only supports TextOutputFormat.
* Hadoop archive

**Hive Optimizations**

A handful of Hive optimizations are not yet included in Spark. Some of these (such as indexes) are
less important due to Spark SQL's in-memory computational model. Others are slotted for future
releases of Spark SQL.

* Block-level bitmap indexes and virtual columns (used to build indexes)
* Automatically determine the number of reducers for joins and groupbys: Currently, in Spark SQL, you
  need to control the degree of parallelism post-shuffle using "`SET spark.sql.shuffle.partitions=[num_tasks];`".
* Meta-data only query: For queries that can be answered by using only metadata, Spark SQL still
  launches tasks to compute the result.
* Skew data flag: Spark SQL does not follow the skew data flags in Hive.
* `STREAMTABLE` hint in join: Spark SQL does not follow the `STREAMTABLE` hint.
* Merge multiple small files for query results: if the result output contains multiple small files,
  Hive can optionally merge the small files into fewer large files to avoid overflowing the HDFS
  metadata. Spark SQL does not support that.

**Hive UDF/UDTF/UDAF**

Not all the APIs of the Hive UDF/UDTF/UDAF are supported by Spark SQL. Below are the unsupported APIs:

* `getRequiredJars` and `getRequiredFiles` (`UDF` and `GenericUDF`) are functions to automatically
  include additional resources required by this UDF.
* `initialize(StructObjectInspector)` in `GenericUDTF` is not supported yet. Spark SQL currently uses
  a deprecated interface `initialize(ObjectInspector[])` only.
* `configure` (`GenericUDF`, `GenericUDTF`, and `GenericUDAFEvaluator`) is a function to initialize
  functions with `MapredContext`, which is inapplicable to Spark.
* `close` (`GenericUDF` and `GenericUDAFEvaluator`) is a function to release associated resources.
  Spark SQL does not call this function when tasks finish.
* `reset` (`GenericUDAFEvaluator`) is a function to re-initialize aggregation for reusing the same aggregation.
  Spark SQL currently does not support the reuse of aggregation.
* `getWindowingEvaluator` (`GenericUDAFEvaluator`) is a function to optimize aggregation by evaluating
  an aggregate over a fixed window.

### Incompatible Hive UDF

Below are the scenarios in which Hive and Spark generate different results:

* `SQRT(n)` If n < 0, Hive returns null, Spark SQL returns NaN.
* `ACOS(n)` If n < -1 or n > 1, Hive returns null, Spark SQL returns NaN.
* `ASIN(n)` If n < -1 or n > 1, Hive returns null, Spark SQL returns NaN.
