---
layout: global
title: Data Sources
displayTitle: Data Sources
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


Spark SQL supports operating on a variety of data sources through the DataFrame interface.
A DataFrame can be operated on using relational transformations and can also be used to create a temporary view.
Registering a DataFrame as a temporary view allows you to run SQL queries over its data. This section
describes the general methods for loading and saving data using the Spark Data Sources and then
goes into specific options that are available for the built-in data sources.


* [Generic Load/Save Functions](sql-data-sources-load-save-functions.html)
  * [Manually Specifying Options](sql-data-sources-load-save-functions.html#manually-specifying-options)
  * [Run SQL on files directly](sql-data-sources-load-save-functions.html#run-sql-on-files-directly)
  * [Save Modes](sql-data-sources-load-save-functions.html#save-modes)
  * [Saving to Persistent Tables](sql-data-sources-load-save-functions.html#saving-to-persistent-tables)
  * [Bucketing, Sorting and Partitioning](sql-data-sources-load-save-functions.html#bucketing-sorting-and-partitioning)
* [Generic File Source Options](sql-data-sources-generic-options.html)
  * [Ignore Corrupt Files](sql-data-sources-generic-options.html#ignore-corrupt-iles)
  * [Ignore Missing Files](sql-data-sources-generic-options.html#ignore-missing-iles)
  * [Path Global Filter](sql-data-sources-generic-options.html#path-global-filter)
  * [Recursive File Lookup](sql-data-sources-generic-options.html#recursive-file-lookup)
* [Parquet Files](sql-data-sources-parquet.html)
  * [Loading Data Programmatically](sql-data-sources-parquet.html#loading-data-programmatically)
  * [Partition Discovery](sql-data-sources-parquet.html#partition-discovery)
  * [Schema Merging](sql-data-sources-parquet.html#schema-merging)
  * [Hive metastore Parquet table conversion](sql-data-sources-parquet.html#hive-metastore-parquet-table-conversion)
  * [Configuration](sql-data-sources-parquet.html#configuration)
* [ORC Files](sql-data-sources-orc.html)
* [JSON Files](sql-data-sources-json.html)
* [CSV Files](sql-data-sources-csv.html)
* [Text Files](sql-data-sources-text.html)
* [Hive Tables](sql-data-sources-hive-tables.html)
  * [Specifying storage format for Hive tables](sql-data-sources-hive-tables.html#specifying-storage-format-for-hive-tables)
  * [Interacting with Different Versions of Hive Metastore](sql-data-sources-hive-tables.html#interacting-with-different-versions-of-hive-metastore)
* [JDBC To Other Databases](sql-data-sources-jdbc.html)
* [Avro Files](sql-data-sources-avro.html)
  * [Deploying](sql-data-sources-avro.html#deploying)
  * [Load and Save Functions](sql-data-sources-avro.html#load-and-save-functions)
  * [to_avro() and from_avro()](sql-data-sources-avro.html#to_avro-and-from_avro)
  * [Data Source Option](sql-data-sources-avro.html#data-source-option)
  * [Configuration](sql-data-sources-avro.html#configuration)
  * [Compatibility with Databricks spark-avro](sql-data-sources-avro.html#compatibility-with-databricks-spark-avro)
  * [Supported types for Avro -> Spark SQL conversion](sql-data-sources-avro.html#supported-types-for-avro---spark-sql-conversion)
  * [Supported types for Spark SQL -> Avro conversion](sql-data-sources-avro.html#supported-types-for-spark-sql---avro-conversion)
* [Whole Binary Files](sql-data-sources-binaryFile.html)
* [Troubleshooting](sql-data-sources-troubleshooting.html)
