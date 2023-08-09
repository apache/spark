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

```sql
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
```

Note that, the clauses between the USING clause and the AS SELECT clause can come in
as any order. For example, you can write COMMENT table_comment after TBLPROPERTIES.

### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **USING data_source**

    Data Source is the input format used to create the table. Data source can be CSV, TXT, ORC, JDBC, PARQUET, etc.

* **OPTIONS**

    Options of data source which will be injected to storage properties.

* **PARTITIONED BY**

    Partitions are created on the table, based on the columns specified.

* **CLUSTERED BY**

    Partitions created on the table will be bucketed into fixed buckets based on the column specified for bucketing.

    **NOTE:** Bucketing is an optimization technique that uses buckets (and bucketing columns) to determine data partitioning and avoid data shuffle.

* **SORTED BY**

    Specifies an ordering of bucket columns. Optionally, one can use ASC for an ascending order or DESC for a descending order after any column names in the SORTED BY clause.
    If not specified, ASC is assumed by default.
   
* **INTO num_buckets BUCKETS**

    Specifies buckets numbers, which is used in `CLUSTERED BY` clause.

* **LOCATION**

    Path to the directory where table data is stored, which could be a path on distributed storage like HDFS, etc.

* **COMMENT**

    A string literal to describe the table.

* **TBLPROPERTIES**

    A list of key-value pairs that is used to tag the table definition.

* **AS select_statement**

    The table is populated using the data from the select statement.

### Data Source Interaction

A Data Source table acts like a pointer to the underlying data source. For example, you can create
a table "foo" in Spark which points to a table "bar" in MySQL using JDBC Data Source. When you
read/write table "foo", you actually read/write table "bar".
 
In general CREATE TABLE is creating a "pointer", and you need to make sure it points to something
existing. An exception is file source such as parquet, json. If you don't specify the LOCATION,
Spark will create a default table location for you.

For CREATE TABLE AS SELECT, Spark will overwrite the underlying data source with the data of the
input query, to make sure the table gets created contains exactly the same data as the input query.

### Examples

```sql

--Use data source
CREATE TABLE student (id INT, name STRING, age INT) USING CSV;

--Use data from another table
CREATE TABLE student_copy USING CSV
    AS SELECT * FROM student;
  
--Omit the USING clause, which uses the default data source (parquet by default)
CREATE TABLE student (id INT, name STRING, age INT);

--Use parquet data source with parquet storage options
--The columns 'id' and 'name' enable the bloom filter during writing parquet file,
--column 'age' does not enable
CREATE TABLE student_parquet(id INT, name STRING, age INT) USING PARQUET
    OPTIONS (
      'parquet.bloom.filter.enabled'='true',
      'parquet.bloom.filter.enabled#age'='false'
    );

--Specify table comment and properties
CREATE TABLE student (id INT, name STRING, age INT) USING CSV
    COMMENT 'this is a comment'
    TBLPROPERTIES ('foo'='bar');

--Specify table comment and properties with different clauses order
CREATE TABLE student (id INT, name STRING, age INT) USING CSV
    TBLPROPERTIES ('foo'='bar')
    COMMENT 'this is a comment';

--Create partitioned and bucketed table
CREATE TABLE student (id INT, name STRING, age INT)
    USING CSV
    PARTITIONED BY (age)
    CLUSTERED BY (Id) INTO 4 buckets;

--Create partitioned and bucketed table through CTAS
CREATE TABLE student_partition_bucket
    USING parquet
    PARTITIONED BY (age)
    CLUSTERED BY (id) INTO 4 buckets
    AS SELECT * FROM student;

--Create bucketed table through CTAS and CTE
CREATE TABLE student_bucket
    USING parquet
    CLUSTERED BY (id) INTO 4 buckets (
    WITH tmpTable AS (
        SELECT * FROM student WHERE id > 100
    )
    SELECT * FROM tmpTable
);
```

### Related Statements

* [CREATE TABLE USING HIVE FORMAT](sql-ref-syntax-ddl-create-table-hiveformat.html)
* [CREATE TABLE LIKE](sql-ref-syntax-ddl-create-table-like.html)
