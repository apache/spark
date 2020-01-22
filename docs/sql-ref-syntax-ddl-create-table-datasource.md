---
layout: global
title: CREATE DATASOURCE TABLE
displayTitle: CREATE DATASOURCE TABLE
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

The `CREATE TABLE` statement defines a new table using a Data Source. 

### Syntax
{% highlight sql %}
CREATE TABLE [ IF NOT EXISTS ] table_identifier
  [ ( col_name1 col_type1 [ COMMENT col_comment1 ], ... ) ]
  USING data_source
  [ OPTIONS ( key1=val1, key2=val2, ... ) ]
  [ PARTITIONED BY ( col_name1, col_name2, ... ) ]
  [ CLUSTERED BY ( col_name3, col_name4, ... ) 
    [ SORTED BY ( col_name [ ASC | DESC ], ... ) ] 
    INTO num_buckets BUCKETS ]
  [ LOCATION path ]
  [ COMMENT table_comment ]
  [ TBLPROPERTIES ( key1=val1, key2=val2, ... ) ]
  [ AS select_statement ]
{% endhighlight %}

### Parameters

<dl>
  <dt><code><em>table_identifier</em></code></dt>
  <dd>
    Specifies a table name, which may be optionally qualified with a database name.<br><br>
    <b>Syntax:</b>
      <code>
        [ database_name. ] table_name
      </code>
  </dd>
</dl>
<dl>
  <dt><code><em>USING data_source</em></code></dt>
  <dd>Data Source is the input format used to create the table. Data source can be CSV, TXT, ORC, JDBC, PARQUET, etc.</dd>
</dl> 

<dl>
  <dt><code><em>PARTITIONED BY</em></code></dt>
  <dd>Partitions are created on the table, based on the columns specified.</dd>
</dl>

<dl>
  <dt><code><em>CLUSTERED BY</em></code></dt>
  <dd>
	Partitions created on the table will be bucketed into fixed buckets based on the column specified for bucketing.<br><br>
   	<b>NOTE:</b>Bucketing is an optimization technique that uses buckets (and bucketing columns) to determine data partitioning and avoid data shuffle.<br>
	<dt><code><em>SORTED BY</em></code></dt>
	<dd>Determines the order in which the data is stored in buckets. Default is Ascending order.</dd>
  </dd>
</dl>

<dl>
  <dt><code><em>LOCATION</em></code></dt>
  <dd>Path to the directory where table data is stored, which could be a path on distributed storage like HDFS, etc.</dd>
</dl>

<dl>
  <dt><code><em>COMMENT</em></code></dt>
  <dd>Table comments are added.</dd>
</dl>

<dl>
  <dt><code><em>TBLPROPERTIES</em></code></dt>
  <dd>Table properties that have to be set are specified, such as `created.by.user`, `owner`, etc.
  </dd>
</dl>

<dl>
  <dt><code><em>AS select_statement</em></code></dt>
  <dd>The table is populated using the data from the select statement.</dd>
</dl>

### Examples
{% highlight sql %}

--Using data source
CREATE TABLE Student (Id INT,name STRING ,age INT) USING CSV;

--Using data from another table
CREATE TABLE StudentInfo
  AS SELECT * FROM Student;

--Partitioned and bucketed
CREATE TABLE Student (Id INT,name STRING ,age INT)
  USING CSV
  PARTITIONED BY (age)
  CLUSTERED BY (Id) INTO 4 buckets;

{% endhighlight %}

### Related Statements
* [CREATE TABLE USING HIVE FORMAT](sql-ref-syntax-ddl-create-table-hiveformat.html)
* [CREATE TABLE LIKE](sql-ref-syntax-ddl-create-table-like.html)
