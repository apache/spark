---
layout: global
displayTitle: Spark SQL, DataFrames and Datasets Guide
title: Spark SQL and DataFrames
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

Spark SQL is a Spark module for structured data processing. Unlike the basic Spark RDD API, the interfaces provided
by Spark SQL provide Spark with more information about the structure of both the data and the computation being performed. Internally,
Spark SQL uses this extra information to perform extra optimizations. There are several ways to
interact with Spark SQL including SQL and the Dataset API. When computing a result,
the same execution engine is used, independent of which API/language you are using to express the
computation. This unification means that developers can easily switch back and forth between
different APIs based on which provides the most natural way to express a given transformation.

All of the examples on this page use sample data included in the Spark distribution and can be run in
the `spark-shell`, `pyspark` shell, or `sparkR` shell.

## SQL

One use of Spark SQL is to execute SQL queries.
Spark SQL can also be used to read data from an existing Hive installation. For more on how to
configure this feature, please refer to the [Hive Tables](sql-data-sources-hive-tables.html) section. When running
SQL from within another programming language the results will be returned as a [Dataset/DataFrame](#datasets-and-dataframes).
You can also interact with the SQL interface using the [command-line](sql-distributed-sql-engine.html#running-the-spark-sql-cli)
or over [JDBC/ODBC](sql-distributed-sql-engine.html#running-the-thrift-jdbcodbc-server).

## Datasets and DataFrames

A Dataset is a distributed collection of data.
Dataset is a new interface added in Spark 1.6 that provides the benefits of RDDs (strong
typing, ability to use powerful lambda functions) with the benefits of Spark SQL's optimized
execution engine. A Dataset can be [constructed](sql-getting-started.html#creating-datasets) from JVM objects and then
manipulated using functional transformations (`map`, `flatMap`, `filter`, etc.).
The Dataset API is available in [Scala][scala-datasets] and
[Java][java-datasets]. Python does not have the support for the Dataset API. But due to Python's dynamic nature,
many of the benefits of the Dataset API are already available (i.e. you can access the field of a row by name naturally
`row.columnName`). The case for R is similar.

A DataFrame is a *Dataset* organized into named columns. It is conceptually
equivalent to a table in a relational database or a data frame in R/Python, but with richer
optimizations under the hood. DataFrames can be constructed from a wide array of [sources](sql-data-sources.html) such
as: structured data files, tables in Hive, external databases, or existing RDDs.
The DataFrame API is available in Scala,
Java, [Python](api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html#pyspark.sql.DataFrame), and [R](api/R/index.html).
In Scala and Java, a DataFrame is represented by a Dataset of `Row`s.
In [the Scala API][scala-datasets], `DataFrame` is simply a type alias of `Dataset[Row]`.
While, in [Java API][java-datasets], users need to use `Dataset<Row>` to represent a `DataFrame`.

[scala-datasets]: api/scala/org/apache/spark/sql/Dataset.html
[java-datasets]: api/java/index.html?org/apache/spark/sql/Dataset.html

Throughout this document, we will often refer to Scala/Java Datasets of `Row`s as DataFrames.
