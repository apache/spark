---
layout: global
title: ANALYZE TABLE
displayTitle: ANALYZE TABLE
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

The `ANALYZE TABLE` statement collects statistics about one specific table or all the tables in one specified database,
that are to be used by the query optimizer to find a better query execution plan.

### Syntax

```sql
ANALYZE TABLE table_identifier [ partition_spec ]
    COMPUTE STATISTICS [ NOSCAN | FOR COLUMNS col [ , ... ] | FOR ALL COLUMNS ]
```

```sql
ANALYZE TABLES [ { FROM | IN } database_name ] COMPUTE STATISTICS [ NOSCAN ]
```

### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    An optional parameter that specifies a comma separated list of key and value pairs
    for partitions. When specified, partition statistics is returned.

    **Syntax:** `PARTITION ( partition_col_name [ = partition_col_val ] [ , ... ] )`

* **{ FROM `|` IN } database_name**

  Specifies the name of the database to be analyzed. Without a database name, `ANALYZE` collects all tables in the current database that the current user has permission to analyze.

* **NOSCAN**

  Collects only the table's size in bytes (which does not require scanning the entire table).

* **FOR COLUMNS col [ , ... ] `|` FOR ALL COLUMNS**

  Collects column statistics for each column specified, or alternatively for every column, as well as table statistics.

If no analyze option is specified, both number of rows and size in bytes are collected.

### Examples

```sql
CREATE DATABASE school_db;
USE school_db;

CREATE TABLE teachers (name STRING, teacher_id INT);
INSERT INTO teachers VALUES ('Tom', 1), ('Jerry', 2);

CREATE TABLE students (name STRING, student_id INT) PARTITIONED BY (student_id);
INSERT INTO students VALUES ('Mark', 111111), ('John', 222222);

ANALYZE TABLE students COMPUTE STATISTICS NOSCAN;

DESC EXTENDED students;
+--------------------+--------------------+-------+
|            col_name|           data_type|comment|
+--------------------+--------------------+-------+
|                name|              string|   null|
|          student_id|                 int|   null|
|                 ...|                 ...|    ...|
|          Statistics|           864 bytes|       |
|                 ...|                 ...|    ...|
+--------------------+--------------------+-------+

ANALYZE TABLE students COMPUTE STATISTICS;

DESC EXTENDED students;
+--------------------+--------------------+-------+
|            col_name|           data_type|comment|
+--------------------+--------------------+-------+
|                name|              string|   null|
|          student_id|                 int|   null|
|                 ...|                 ...|    ...|
|          Statistics|   864 bytes, 2 rows|       |
|                 ...|                 ...|    ...|
+--------------------+--------------------+-------+

ANALYZE TABLE students PARTITION (student_id = 111111) COMPUTE STATISTICS;

DESC EXTENDED students PARTITION (student_id = 111111);
+--------------------+--------------------+-------+
|            col_name|           data_type|comment|
+--------------------+--------------------+-------+
|                name|              string|   null|
|          student_id|                 int|   null|
|                 ...|                 ...|    ...|
|Partition Statistics|   432 bytes, 1 rows|       |
|                 ...|                 ...|    ...|
+--------------------+--------------------+-------+

ANALYZE TABLE students COMPUTE STATISTICS FOR COLUMNS name;

DESC EXTENDED students name;
+--------------+----------+
|     info_name|info_value|
+--------------+----------+
|      col_name|      name|
|     data_type|    string|
|       comment|      NULL|
|           min|      NULL|
|           max|      NULL|
|     num_nulls|         0|
|distinct_count|         2|
|   avg_col_len|         4|
|   max_col_len|         4|
|     histogram|      NULL|
+--------------+----------+

ANALYZE TABLES IN school_db COMPUTE STATISTICS NOSCAN;

DESC EXTENDED teachers;
+--------------------+--------------------+-------+
|            col_name|           data_type|comment|
+--------------------+--------------------+-------+
|                name|              string|   null|
|          teacher_id|                 int|   null|
|                 ...|                 ...|    ...|
|          Statistics|          1382 bytes|       |
|                 ...|                 ...|    ...|
+--------------------+--------------------+-------+

DESC EXTENDED students;
+--------------------+--------------------+-------+
|            col_name|           data_type|comment|
+--------------------+--------------------+-------+
|                name|              string|   null|
|          student_id|                 int|   null|
|                 ...|                 ...|    ...|
|          Statistics|           864 bytes|       |
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
|          Statistics|  1382 bytes, 2 rows|       |
|                 ...|                 ...|    ...|
+--------------------+--------------------+-------+

DESC EXTENDED students;
+--------------------+--------------------+-------+
|            col_name|           data_type|comment|
+--------------------+--------------------+-------+
|                name|              string|   null|
|          student_id|                 int|   null|
|                 ...|                 ...|    ...|
|          Statistics|   864 bytes, 2 rows|       |
|                 ...|                 ...|    ...|
+--------------------+--------------------+-------+
```
