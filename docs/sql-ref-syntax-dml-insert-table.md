---
layout: global
title: INSERT TABLE
displayTitle: INSERT TABLE
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

The `INSERT` statement inserts new rows into a table or overwrites the existing data in the table. The inserted rows can be specified by value expressions or result from a query.

### Syntax

```sql
INSERT [ INTO | OVERWRITE ] [ TABLE ] table_identifier [ partition_spec ] [ ( column_list ) | [BY NAME] ]
    { VALUES ( { value | NULL } [ , ... ] ) [ , ( ... ) ] | query }

INSERT INTO [ TABLE ] table_identifier REPLACE WHERE boolean_expression query
```

### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    An optional parameter that specifies a comma-separated list of key and value pairs
    for partitions. Note that one can use a typed literal (e.g., date'2019-01-02') in the partition spec.

    **Syntax:** `PARTITION ( partition_col_name  = partition_col_val [ , ... ] )`

* **column_list**

    An optional parameter that specifies a comma-separated list of columns belonging to the `table_identifier` table. Spark will reorder the columns of the input query to match the table schema according to the specified column list.

    **Note:**The current behaviour has some limitations:
    - All specified columns should exist in the table and not be duplicated from each other. It includes all columns except the static partition columns.
    - The size of the column list should be exactly the size of the data from `VALUES` clause or query.

* **VALUES ( { value `|` NULL } [ , ... ] ) [ , ( ... ) ]**

    Specifies the values to be inserted. Either an explicitly specified value or a NULL can be inserted.
    A comma must be used to separate each value in the clause. More than one set of values can be specified to insert multiple rows.

* **boolean_expression**

  Specifies any expression that evaluates to a result type `boolean`. Two or
  more expressions may be combined together using the logical
  operators ( `AND`, `OR` ).

* **query**

    A query that produces the rows to be inserted. It can be in one of following formats:
    * a [SELECT](sql-ref-syntax-qry-select.html) statement
    * a [Inline Table](sql-ref-syntax-qry-select-inline-table.html) statement
    * a `FROM` statement

### Examples

#### Insert Into

##### Single Row Insert Using a VALUES Clause

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

##### Multi-Row Insert Using a VALUES Clause

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

##### Insert Using a SELECT Statement

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

##### Insert Using a TABLE Statement

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

##### Insert Using a FROM Statement

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
     FROM applicants SELECT name, address, student_id WHERE qualified = true;

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

##### Insert Using a Typed Date Literal for a Partition Column Value
```sql
CREATE TABLE students (name STRING, address  STRING) PARTITIONED BY (birthday DATE);

INSERT INTO students PARTITION (birthday = date'2019-01-02')
    VALUES ('Amy Smith', '123 Park Ave, San Jose');

SELECT * FROM students;
+-------------+-------------------------+-----------+
|         name|                  address|   birthday|
+-------------+-------------------------+-----------+
|    Amy Smith|   123 Park Ave, San Jose| 2019-01-02|
+-------------+-------------------------+-----------+
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

##### Insert with both a partition spec and a column list

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

#### Insert Overwrite

##### Insert Using a VALUES Clause

```sql
-- Assuming the students table has already been created and populated.
SELECT * FROM students;
+-------------+--------------------------+----------+
|         name|                   address|student_id|
+-------------+--------------------------+----------+
|    Amy Smith|    123 Park Ave, San Jose|    111111|
|    Bob Brown|  456 Taylor St, Cupertino|    222222|
|Cathy Johnson|   789 Race Ave, Palo Alto|    333333|
|Dora Williams|134 Forest Ave, Menlo Park|    444444|
|Fleur Laurent|     345 Copper St, London|    777777|
|Gordon Martin|      779 Lake Ave, Oxford|    888888|
|  Helen Davis| 469 Mission St, San Diego|    999999|
|   Jason Wang|     908 Bird St, Saratoga|    121212|
+-------------+--------------------------+----------+

INSERT OVERWRITE students VALUES
    ('Ashua Hill', '456 Erica Ct, Cupertino', 111111),
    ('Brian Reed', '723 Kern Ave, Palo Alto', 222222);

SELECT * FROM students;
+----------+-----------------------+----------+
|      name|                address|student_id|
+----------+-----------------------+----------+
|Ashua Hill|456 Erica Ct, Cupertino|    111111|
|Brian Reed|723 Kern Ave, Palo Alto|    222222|
+----------+-----------------------+----------+
```

##### Insert Using a SELECT Statement

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

INSERT OVERWRITE students PARTITION (student_id = 222222)
    SELECT name, address FROM persons WHERE name = "Dora Williams";

SELECT * FROM students;
+-------------+--------------------------+----------+
|         name|                   address|student_id|
+-------------+--------------------------+----------+
|   Ashua Hill|   456 Erica Ct, Cupertino|    111111|
+-------------+--------------------------+----------+
|Dora Williams|134 Forest Ave, Menlo Park|    222222|
+-------------+--------------------------+----------+
```

##### Insert By Name Using a SELECT Statement

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

-- Spark will reorder the fields of the query according to the order of the fields in the table,
-- so don't worry about the field order mismatch
INSERT INTO students PARTITION (student_id = 222222) BY NAME
    SELECT address, name FROM persons WHERE name = "Dora Williams";

SELECT * FROM students;
+-------------+--------------------------+----------+
|         name|                   address|student_id|
+-------------+--------------------------+----------+
|   Ashua Hill|   456 Erica Ct, Cupertino|    111111|
+-------------+--------------------------+----------+
|Dora Williams|134 Forest Ave, Menlo Park|    222222|
+-------------+--------------------------+----------+
    
INSERT OVERWRITE students PARTITION (student_id = 222222) BY NAME
    SELECT 'Unknown' as address, name FROM persons WHERE name = "Dora Williams";

SELECT * FROM students;
+-------------+--------------------------+----------+
|         name|                   address|student_id|
+-------------+--------------------------+----------+
|   Ashua Hill|   456 Erica Ct, Cupertino|    111111|
+-------------+--------------------------+----------+
|Dora Williams|                   Unknown|    222222|
+-------------+--------------------------+----------+
```

##### Insert Using a REPLACE WHERE Statement

```sql
-- Assuming the persons and persons2 table has already been created and populated.
SELECT * FROM persons;
+-------------+--------------------------+---------+
|         name|                   address|      ssn|
+-------------+--------------------------+---------+
|Dora Williams|134 Forest Ave, Menlo Park|123456789|
+-------------+--------------------------+---------+
|  Eddie Davis|   245 Market St, Milpitas|345678901|
+-------------+--------------------------+---------+
    
SELECT * FROM persons2;
+-------------+--------------------------+---------+
|         name|                   address|      ssn|
+-------------+--------------------------+---------+
|   Ashua Hill|   456 Erica Ct, Cupertino|432795921|
+-------------+--------------------------+---------+

-- in an atomic operation, 1) delete rows with ssn = 123456789 and 2) insert rows from persons2 
INSERT INTO persons REPLACE WHERE ssn = 123456789 SELECT * FROM persons2;

SELECT * FROM persons;
+-------------+--------------------------+---------+
|         name|                   address|      ssn|
+-------------+--------------------------+---------+
|  Eddie Davis|   245 Market St, Milpitas|345678901|
+-------------+--------------------------+---------+
|   Ashua Hill|   456 Erica Ct, Cupertino|432795921|
+-------------+--------------------------+---------+
```

##### Insert Using a TABLE Statement

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

INSERT OVERWRITE students TABLE visiting_students;

SELECT * FROM students;
+-------------+---------------------+----------+
|         name|              address|student_id|
+-------------+---------------------+----------+
|Fleur Laurent|345 Copper St, London|    777777|
+-------------+---------------------+----------+
|Gordon Martin| 779 Lake Ave, Oxford|    888888|
+-------------+---------------------+----------+
```

##### Insert Using a FROM Statement

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

INSERT OVERWRITE students
    FROM applicants SELECT name, address, student_id WHERE qualified = true;

SELECT * FROM students;
+-----------+-------------------------+----------+
|       name|                  address|student_id|
+-----------+-------------------------+----------+
|Helen Davis|469 Mission St, San Diego|    999999|
+-----------+-------------------------+----------+
| Jason Wang|    908 Bird St, Saratoga|    121212|
+-----------+-------------------------+----------+
```

##### Insert Using a Typed Date Literal for a Partition Column Value

```sql
CREATE TABLE students (name STRING, address  STRING) PARTITIONED BY (birthday DATE);

INSERT INTO students PARTITION (birthday = date'2019-01-02')
    VALUES ('Amy Smith', '123 Park Ave, San Jose');

SELECT * FROM students;
+-------------+-------------------------+-----------+
|         name|                  address|   birthday|
+-------------+-------------------------+-----------+
|    Amy Smith|   123 Park Ave, San Jose| 2019-01-02|
+-------------+-------------------------+-----------+

INSERT OVERWRITE students PARTITION (birthday = date'2019-01-02')
    VALUES('Jason Wang', '908 Bird St, Saratoga');

SELECT * FROM students;
+-----------+-------------------------+-----------+
|       name|                  address|   birthday|
+-----------+-------------------------+-----------+
| Jason Wang|    908 Bird St, Saratoga| 2019-01-02|
+-----------+-------------------------+-----------+
```

##### Insert with a column list

```sql
INSERT OVERWRITE students (address, name, student_id) VALUES
    ('Hangzhou, China', 'Kent Yao', 11215016);

SELECT * FROM students WHERE name = 'Kent Yao';
+---------+----------------------+----------+
|     name|               address|student_id|
+---------+----------------------+----------+
|Kent Yao |       Hangzhou, China|  11215016|
+---------+----------------------+----------+
```

##### Insert with both a partition spec and a column list

```sql
INSERT OVERWRITE students PARTITION (student_id = 11215016) (address, name) VALUES
    ('Hangzhou, China', 'Kent Yao Jr.');

SELECT * FROM students WHERE student_id = 11215016;
+------------+----------------------+----------+
|        name|               address|student_id|
+------------+----------------------+----------+
|Kent Yao Jr.|       Hangzhou, China|  11215016|
+------------+----------------------+----------+
```

### Related Statements

* [INSERT OVERWRITE DIRECTORY statement](sql-ref-syntax-dml-insert-overwrite-directory.html)
