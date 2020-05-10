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

```sql
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
```

Note that, the clauses between the columns definition clause and the AS SELECT clause can come in
as any order. For example, you can write COMMENT table_comment after TBLPROPERTIES.

### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **EXTERNAL**

    Table is defined using the path provided as LOCATION, does not use default location for this table.

* **PARTITIONED BY**

    Partitions are created on the table, based on the columns specified.

* **ROW FORMAT**

    SERDE is used to specify a custom SerDe or the DELIMITED clause in order to use the native SerDe.

* **STORED AS**

    File format for table storage, could be TEXTFILE, ORC, PARQUET, etc.

* **LOCATION**

    Path to the directory where table data is stored, which could be a path on distributed storage like HDFS, etc.

* **COMMENT**

    A string literal to describe the table.

* **TBLPROPERTIES**

    A list of key-value pairs that is used to tag the table definition.

* **AS select_statement**

    The table is populated using the data from the select statement.

### Examples

```sql
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
```

### Related Statements

* [CREATE TABLE USING DATASOURCE](sql-ref-syntax-ddl-create-table-datasource.html)
* [CREATE TABLE LIKE](sql-ref-syntax-ddl-create-table-like.html)
