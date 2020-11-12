---
layout: global
title: DESCRIBE QUERY
displayTitle: DESCRIBE QUERY
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

The `DESCRIBE QUERY` statement is used to return the metadata of output
of a query. A shorthand `DESC` may be used instead of `DESCRIBE` to
describe the query output.

### Syntax

```sql
{ DESC | DESCRIBE } [ QUERY ] input_statement
```

### Parameters

* **QUERY**
    This clause is optional and may be omitted.

* **input_statement**

    Specifies a result set producing statement and may be one of the following: 

    * a `SELECT` statement
    * a `CTE(Common table expression)` statement
    * an `INLINE TABLE` statement
    * a `TABLE` statement
    * a `FROM` statement`

    Please refer to [select-statement](sql-ref-syntax-qry-select.html)
    for a detailed syntax of the query parameter.

### Examples

```sql
-- Create table `person`
CREATE TABLE person (name STRING , age INT COMMENT 'Age column', address STRING);

-- Returns column metadata information for a simple select query
DESCRIBE QUERY SELECT age, sum(age) FROM person GROUP BY age;
+--------+---------+----------+
|col_name|data_type|   comment|
+--------+---------+----------+
|     age|      int|Age column|
|sum(age)|   bigint|      null|
+--------+---------+----------+

-- Returns column metadata information for common table expression (`CTE`).
DESCRIBE QUERY WITH all_names_cte
    AS (SELECT name from person) SELECT * FROM all_names_cte;
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|    name|   string|   null|
+--------+---------+-------+

-- Returns column metadata information for an inline table.
DESC QUERY VALUES(100, 'John', 10000.20D) AS employee(id, name, salary);
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|      id|      int|   null|
|    name|   string|   null|
|  salary|   double|   null|
+--------+---------+-------+

-- Returns column metadata information for `TABLE` statement.
DESC QUERY TABLE person;
+--------+---------+----------+
|col_name|data_type|   comment|
+--------+---------+----------+
|    name|   string|      null|
|     age|      int| Agecolumn|
| address|   string|      null|
+--------+---------+----------+

-- Returns column metadata information for a `FROM` statement.
-- `QUERY` clause is optional and can be omitted.
DESCRIBE FROM person SELECT age;
+--------+---------+----------+
|col_name|data_type|   comment|
+--------+---------+----------+
|     age|      int| Agecolumn|
+--------+---------+----------+
```

### Related Statements

* [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
* [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
* [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
