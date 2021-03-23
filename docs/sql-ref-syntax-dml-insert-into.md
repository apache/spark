---
layout: global
title: INSERT INTO
displayTitle: INSERT INTO
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

The `INSERT INTO` statement inserts new rows into a table. The inserted rows can be specified by value expressions or result from a query.

### Syntax

```sql
INSERT INTO [ TABLE ] table_identifier [ partition_spec ] [ ( column_list ) ]
    { VALUES ( { value | NULL } [ , ... ] ) [ , ( ... ) ] | query }
```

### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    An optional parameter that specifies a comma-separated list of key and value pairs
    for partitions.

    **Syntax:** `PARTITION ( partition_col_name  = partition_col_val [ , ... ] )`

* **column_list**

    An optional parameter that specifies a comma-separated list of columns belonging to the `table_identifier` table. Spark will reorder the columns of the input query to match the table schema according to the specified column list.

    **Note:**The current behaviour has some limitations:
    - All specified columns should exist in the table and not be duplicated from each other. It includes all columns except the static partition columns.
    - The size of the column list should be exactly the size of the data from `VALUES` clause or query.

* **VALUES ( { value `|` NULL } [ , ... ] ) [ , ( ... ) ]**

    Specifies the values to be inserted. Either an explicitly specified value or a NULL can be inserted.
    A comma must be used to separate each value in the clause. More than one set of values can be specified to insert multiple rows.

* **query**

    A query that produces the rows to be inserted. It can be in one of following formats:
    * a `SELECT` statement
    * a `TABLE` statement
    * a `FROM` statement

### Examples

#### Single Row Insert Using a VALUES Clause

```sql
CREATE TABLE students (name VARCHAR(64), address VARCHAR(64))
    USING PARQUET PARTITIONED BY (student_id INT);

INSERT INTO students VALUES
    ('Amy Smith', '123 Park Ave, San Jose', 111111);

SELECT * FROM students;
+---------+----------------------+----------+
|     name|               address|student_id|
+---------+----------------------+----------+
|Amy Smith|123 Park Ave, San Jose|    111111|
+---------+----------------------+----------+
```

#### Multi-Row Insert Using a VALUES Clause

```sql
INSERT INTO students VALUES
    ('Bob Brown', '456 Taylor St, Cupertino', 222222),
    ('Cathy Johnson', '789 Race Ave, Palo Alto', 333333);

SELECT * FROM students;
+-------------+------------------------+----------+
|         name|                 address|student_id|
+-------------+------------------------+----------+
|    Amy Smith|  123 Park Ave, San Jose|    111111|
+-------------+------------------------+----------+
|    Bob Brown|456 Taylor St, Cupertino|    222222|
+-------------+------------------------+----------+
|Cathy Johnson| 789 Race Ave, Palo Alto|    333333|
+--------------+-----------------------+----------+
```

#### Insert Using a SELECT Statement

```sql
-- Assuming the persons table has already been created and populated.
SELECT * FROM persons;
+-------------+--------------------------+---------+
|         name|                   address|      ssn|
+-------------+--------------------------+---------+
|Dora Williams|134 Forest Ave, Menlo Park|123456789|
+-------------+--------------------------+---------+
|  Eddie Davis|   245 Market St, Milpitas|345678901|
+-------------+--------------------------+---------+

INSERT INTO students PARTITION (student_id = 444444)
    SELECT name, address FROM persons WHERE name = "Dora Williams";

SELECT * FROM students;
+-------------+--------------------------+----------+
|         name|                   address|student_id|
+-------------+--------------------------+----------+
|    Amy Smith|    123 Park Ave, San Jose|    111111|
+-------------+--------------------------+----------+
|    Bob Brown|  456 Taylor St, Cupertino|    222222|
+-------------+--------------------------+----------+
|Cathy Johnson|   789 Race Ave, Palo Alto|    333333|
+-------------+--------------------------+----------+
|Dora Williams|134 Forest Ave, Menlo Park|    444444|
+-------------+--------------------------+----------+
```

#### Insert Using a TABLE Statement

```sql
-- Assuming the visiting_students table has already been created and populated.
SELECT * FROM visiting_students;
+-------------+---------------------+----------+
|         name|              address|student_id|
+-------------+---------------------+----------+
|Fleur Laurent|345 Copper St, London|    777777|
+-------------+---------------------+----------+
|Gordon Martin| 779 Lake Ave, Oxford|    888888|
+-------------+---------------------+----------+

INSERT INTO students TABLE visiting_students;

SELECT * FROM students;
+-------------+--------------------------+----------+
|         name|                   address|student_id|
+-------------+--------------------------+----------+
|    Amy Smith|    123 Park Ave, San Jose|    111111|
+-------------+--------------------------+----------+
|    Bob Brown|  456 Taylor St, Cupertino|    222222|
+-------------+--------------------------+----------+
|Cathy Johnson|   789 Race Ave, Palo Alto|    333333|
+-------------+--------------------------+----------+
|Dora Williams|134 Forest Ave, Menlo Park|    444444|
+-------------+--------------------------+----------+
|Fleur Laurent|     345 Copper St, London|    777777|
+-------------+--------------------------+----------+
|Gordon Martin|      779 Lake Ave, Oxford|    888888|
+-------------+--------------------------+----------+
```

#### Insert Using a FROM Statement

```sql
-- Assuming the applicants table has already been created and populated.
SELECT * FROM applicants;
+-----------+--------------------------+----------+---------+
|       name|                   address|student_id|qualified|
+-----------+--------------------------+----------+---------+
|Helen Davis| 469 Mission St, San Diego|    999999|     true|
+-----------+--------------------------+----------+---------+
|   Ivy King|367 Leigh Ave, Santa Clara|    101010|    false|
+-----------+--------------------------+----------+---------+
| Jason Wang|     908 Bird St, Saratoga|    121212|     true|
+-----------+--------------------------+----------+---------+

INSERT INTO students
     FROM applicants SELECT name, address, id applicants WHERE qualified = true;

SELECT * FROM students;
+-------------+--------------------------+----------+
|         name|                   address|student_id|
+-------------+--------------------------+----------+
|    Amy Smith|    123 Park Ave, San Jose|    111111|
+-------------+--------------------------+----------+
|    Bob Brown|  456 Taylor St, Cupertino|    222222|
+-------------+--------------------------+----------+
|Cathy Johnson|   789 Race Ave, Palo Alto|    333333|
+-------------+--------------------------+----------+
|Dora Williams|134 Forest Ave, Menlo Park|    444444|
+-------------+--------------------------+----------+
|Fleur Laurent|     345 Copper St, London|    777777|
+-------------+--------------------------+----------+
|Gordon Martin|      779 Lake Ave, Oxford|    888888|
+-------------+--------------------------+----------+
|  Helen Davis| 469 Mission St, San Diego|    999999|
+-------------+--------------------------+----------+
|   Jason Wang|     908 Bird St, Saratoga|    121212|
+-------------+--------------------------+----------+
```

#### Insert with a column list

```sql
INSERT INTO students (address, name, student_id) VALUES
    ('Hangzhou, China', 'Kent Yao', 11215016);

SELECT * FROM students WHERE name = 'Kent Yao';
+---------+----------------------+----------+
|     name|               address|student_id|
+---------+----------------------+----------+
|Kent Yao |       Hangzhou, China|  11215016|
+---------+----------------------+----------+
```

#### Insert with both a partition spec and a column list

```sql
INSERT INTO students PARTITION (student_id = 11215017) (address, name) VALUES
    ('Hangzhou, China', 'Kent Yao Jr.');

SELECT * FROM students WHERE student_id = 11215017;
+------------+----------------------+----------+
|        name|               address|student_id|
+------------+----------------------+----------+
|Kent Yao Jr.|       Hangzhou, China|  11215017|
+------------+----------------------+----------+
```

### Related Statements

* [INSERT OVERWRITE statement](sql-ref-syntax-dml-insert-overwrite-table.html)
* [INSERT OVERWRITE DIRECTORY statement](sql-ref-syntax-dml-insert-overwrite-directory.html)
* [INSERT OVERWRITE DIRECTORY with Hive format statement](sql-ref-syntax-dml-insert-overwrite-directory-hive.html)
