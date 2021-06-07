---
layout: global
title: ANALYZE TABLES
displayTitle: ANALYZE TABLES
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

The `ANALYZE TABLES` statement collects statistics about all the tables in a specified database to be used by the query optimizer to find a better query execution plan.

### Syntax

```sql
ANALYZE TABLES [ { FROM | IN } database_name ] COMPUTE STATISTICS [ NOSCAN ]
```

### Parameters

* **{ FROM `|` IN } database_name**

    Specifies the name of the database to be analyzed. Without a database name, `ANALYZE` collects all tables in the current database that the current user has permission to analyze.

* **[ NOSCAN ]**

    Collects only the table's size in bytes (which does not require scanning the entire table).

### Examples

```sql
CREATE DATABASE school_db;
USE school_db;

CREATE TABLE teachers (name STRING, teacher_id INT);
INSERT INTO teachers VALUES ('Tom', 1), ('Jerry', 2);

CREATE TABLE students (name STRING, student_id INT, age SHORT);
INSERT INTO students VALUES ('Mark', 111111, 10), ('John', 222222, 11);

ANALYZE TABLES IN school_db COMPUTE STATISTICS NOSCAN;

DESC EXTENDED teachers;
+--------------------+--------------------+-------+
|            col_name|           data_type|comment|
+--------------------+--------------------+-------+
|                name|              string|   null|
|          teacher_id|                 int|   null|
|                 ...|                 ...|    ...|
|            Provider|             parquet|       |
|          Statistics|          1382 bytes|       |
|                 ...|                 ...|    ...|
+--------------------+--------------------+-------+

DESC EXTENDED students;
+--------------------+--------------------+-------+
|            col_name|           data_type|comment|
+--------------------+--------------------+-------+
|                name|              string|   null|
|          student_id|                 int|   null|
|                 age|            smallint|   null|
|                 ...|                 ...|    ...|
|          Statistics|          1828 bytes|       |
|                 ...|                 ...|    ...|
+--------------------+--------------------+-------+

ANALYZE TABLES COMPUTE STATISTICS;

DESC EXTENDED teachers;
+--------------------+--------------------+-------+
|            col_name|           data_type|comment|
+--------------------+--------------------+-------+
|                name|              string|   null|
|          teacher_id|                 int|   null|
|                 ...|                 ...|    ...|
|            Provider|             parquet|       |
|          Statistics|  1382 bytes, 2 rows|       |
|                 ...|                 ...|    ...|
+--------------------+--------------------+-------+

DESC EXTENDED students;
+--------------------+--------------------+-------+
|            col_name|           data_type|comment|
+--------------------+--------------------+-------+
|                name|              string|   null|
|          student_id|                 int|   null|
|                 age|            smallint|   null|
|                 ...|                 ...|    ...|
|            Provider|             parquet|       |
|          Statistics|  1828 bytes, 2 rows|       |
|                 ...|                 ...|    ...|
+--------------------+--------------------+-------+
```

### Related Statements

* [ANALYZE TABLE](sql-ref-syntax-aux-analyze-table.html)
