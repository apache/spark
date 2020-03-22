---
layout: global
title: CREATE HIVEFORMAT TABLE
displayTitle: CREATE HIVEFORMAT TABLE
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

The `CREATE TABLE` statement defines a new table using Hive format.

### Syntax
{% highlight sql %}
CREATE [ EXTERNAL ] TABLE [ IF NOT EXISTS ] table_identifier
  [ ( col_name1[:] col_type1 [ COMMENT col_comment1 ], ... ) ]
  [ COMMENT table_comment ]
  [ PARTITIONED BY ( col_name2[:] col_type2 [ COMMENT col_comment2 ], ... ) 
      | ( col_name1, col_name2, ... ) ]
  [ ROW FORMAT row_format ]
  [ STORED AS file_format ]
  [ LOCATION path ]
  [ TBLPROPERTIES ( key1=val1, key2=val2, ... ) ]
  [ AS select_statement ]

{% endhighlight %}

Note that, the clauses between the columns definition clause and the AS SELECT clause can come in
as any order. For example, you can write COMMENT table_comment after TBLPROPERTIES.

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
  <dt><code><em>EXTERNAL</em></code></dt>
  <dd>Table is defined using the path provided as LOCATION, does not use default location for this table.</dd>
</dl>

<dl>
  <dt><code><em>PARTITIONED BY</em></code></dt>
  <dd>Partitions are created on the table, based on the columns specified.</dd>
</dl>

<dl>
  <dt><code><em>ROW FORMAT</em></code></dt>
  <dd>SERDE is used to specify a custom SerDe or the DELIMITED clause in order to use the native SerDe.</dd>
</dl>

<dl>
  <dt><code><em>STORED AS</em></code></dt>
    <dd>File format for table storage, could be TEXTFILE, ORC, PARQUET,etc.</dd>
</dl>

<dl>
  <dt><code><em>LOCATION</em></code></dt>
  <dd>Path to the directory where table data is stored, Path to the directory where table data is stored, which could be a path on distributed storage like HDFS, etc.</dd>
</dl>

<dl>
  <dt><code><em>COMMENT</em></code></dt>
  <dd>A string literal to describe the table.</dd>
</dl>

<dl>
  <dt><code><em>TBLPROPERTIES</em></code></dt>
  <dd>A list of key-value pairs that is used to tag the table definition.</dd>
</dl>

<dl>
  <dt><code><em>AS select_statement</em></code></dt>
  <dd>The table is populated using the data from the select statement.</dd>
</dl>


### Examples
{% highlight sql %}

--Use hive format
CREATE TABLE student (id INT, name STRING, age INT) STORED AS ORC;

--Use data from another table
CREATE TABLE student_copy STORED AS ORC
  AS SELECT * FROM student;

--Specify table comment and properties
CREATE TABLE student (id INT, name STRING, age INT)
  COMMENT 'this is a comment'
  STORED AS ORC
  TBLPROPERTIES ('foo'='bar');  

--Specify table comment and properties with different clauses order
CREATE TABLE student (id INT, name STRING, age INT)
  STORED AS ORC
  TBLPROPERTIES ('foo'='bar')
  COMMENT 'this is a comment';

--Create partitioned table
CREATE TABLE student (id INT, name STRING)
  PARTITIONED BY (age INT)
  STORED AS ORC;

--Create partitioned table with different clauses order
CREATE TABLE student (id INT, name STRING)
  STORED AS ORC
  PARTITIONED BY (age INT);

--Use Row Format and file format
CREATE TABLE student (id INT,name STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE;

{% endhighlight %}


### Related Statements
* [CREATE TABLE USING DATASOURCE](sql-ref-syntax-ddl-create-table-datasource.html)
* [CREATE TABLE LIKE](sql-ref-syntax-ddl-create-table-like.html)
