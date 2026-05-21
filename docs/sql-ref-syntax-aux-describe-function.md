---
layout: global
title: DESCRIBE FUNCTION
displayTitle: DESCRIBE FUNCTION
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

`DESCRIBE FUNCTION` statement returns the basic metadata information of an
existing function. For built-in and external (Java/Hive) functions the output includes the
function name, implementing class, and usage details. For
[SQL user-defined functions](sql-ref-syntax-ddl-create-sql-function.html) the output describes
the function signature (input parameters, return type/columns) and, with `EXTENDED`, the
function body, characteristics, and the frozen
[SQL Path](sql-ref-name-resolution.html#sql-path) that was captured at creation time.

If the optional `EXTENDED` option is specified, the basic metadata is returned along with the
extended information.

### Syntax

```sql
{ DESC | DESCRIBE } FUNCTION [ EXTENDED ] function_name
```

### Parameters

* **function_name**

    Specifies a name of an existing function. The function name follows the regular
    [name resolution](sql-ref-name-resolution.html#function-resolution) rules: unqualified
    names walk the SQL Path; 2- and 3-part names may use the `system.builtin` / `system.session`
    namespaces (or their shortcuts `builtin` / `session`).

    **Syntax:** `[ catalog_name. ] [ database_name. ] function_name`

### Examples

```sql
-- Describe a builtin scalar function.
-- Returns function name, implementing class and usage
DESC FUNCTION abs;
+-------------------------------------------------------------------+
|function_desc                                                      |
+-------------------------------------------------------------------+
|Function: abs                                                      |
|Class: org.apache.spark.sql.catalyst.expressions.Abs               |
|Usage: abs(expr) - Returns the absolute value of the numeric value.|
+-------------------------------------------------------------------+

-- Describe a builtin scalar function.
-- Returns function name, implementing class and usage and examples.
DESC FUNCTION EXTENDED abs;
+-------------------------------------------------------------------+
|function_desc                                                      |
+-------------------------------------------------------------------+
|Function: abs                                                      |
|Class: org.apache.spark.sql.catalyst.expressions.Abs               |
|Usage: abs(expr) - Returns the absolute value of the numeric value.|
|Extended Usage:                                                    |
|    Examples:                                                      |
|      > SELECT abs(-1);                                            |
|       1                                                           |
|                                                                   |
+-------------------------------------------------------------------+

-- Describe a builtin aggregate function
DESC FUNCTION max;
+--------------------------------------------------------------+
|function_desc                                                 |
+--------------------------------------------------------------+
|Function: max                                                 |
|Class: org.apache.spark.sql.catalyst.expressions.aggregate.Max|
|Usage: max(expr) - Returns the maximum value of `expr`.       |
+--------------------------------------------------------------+

-- Describe a builtin user defined aggregate function
-- Returns function name, implementing class and usage and examples.
DESC FUNCTION EXTENDED explode;
+---------------------------------------------------------------+
|function_desc                                                  |
+---------------------------------------------------------------+
|Function: explode                                              |
|Class: org.apache.spark.sql.catalyst.expressions.Explode       |
|Usage: explode(expr) - Separates the elements of array `expr`  |
| into multiple rows, or the elements of map `expr` into        |
| multiple rows and columns. Unless specified otherwise, uses   |
| the default column name `col` for elements of the array or    |
| `key` and `value` for the elements of the map.                |
|Extended Usage:                                                |
|    Examples:                                                  |
|      > SELECT explode(array(10, 20));                         |
|       10                                                      |
|       20                                                      |
+---------------------------------------------------------------+

-- Built-in functions can be qualified with `builtin` or `system.builtin`.
DESC FUNCTION system.builtin.abs;
+-------------------------------------------------------------------+
|function_desc                                                      |
+-------------------------------------------------------------------+
|Function: abs                                                      |
|Class: org.apache.spark.sql.catalyst.expressions.Abs               |
|Usage: abs(expr) - Returns the absolute value of the numeric value.|
+-------------------------------------------------------------------+

-- Describe a SQL scalar UDF: the output uses the SQL function layout
-- (Function / Type / Input / Returns).
CREATE FUNCTION area(x DOUBLE, y DOUBLE) RETURNS DOUBLE RETURN x * y;
DESC FUNCTION area;
+-------------------------------+
|function_desc                  |
+-------------------------------+
|Function: spark_catalog.default.area|
|Type:     SCALAR               |
|Input:    x DOUBLE             |
|          y DOUBLE             |
|Returns:  DOUBLE               |
+-------------------------------+

-- Describe a SQL table UDF.
CREATE FUNCTION getemps(deptno INT)
  RETURNS TABLE (id INT, name STRING)
  RETURN SELECT id, name FROM employee WHERE employee.deptno = getemps.deptno;
DESC FUNCTION getemps;
+--------------------------------------+
|function_desc                         |
+--------------------------------------+
|Function: spark_catalog.default.getemps|
|Type:     TABLE                       |
|Input:    deptno INT                  |
|Returns:  id   INT                    |
|          name STRING                 |
+--------------------------------------+

-- DESC FUNCTION EXTENDED for a SQL UDF adds the body, the characteristic clauses,
-- the captured SQL configs, the owner, the create time, and the frozen SQL Path
-- (when `spark.sql.path.enabled` is true).
SET spark.sql.path.enabled = true;
SET PATH = spark_catalog.default, system.builtin;
CREATE FUNCTION frozen_fn() RETURNS INT
  COMMENT 'demo function'
  RETURN (SELECT MAX(id) FROM frozen_t);
DESC FUNCTION EXTENDED frozen_fn;
+-----------------------------------------------------------------+
|function_desc                                                    |
+-----------------------------------------------------------------+
|Function:     spark_catalog.default.frozen_fn                    |
|Type:         SCALAR                                             |
|Input:        ()                                                 |
|Returns:      INT                                                |
|Comment:      demo function                                      |
|Deterministic:false                                              |
|Data Access:  READS SQL DATA                                     |
|Configs:      spark.sql.ansi.enabled=true                        |
|              ...                                                |
|Owner:        <USER>                                             |
|Create Time:  Wed Apr 30 14:05:43 PDT 2026                       |
|Body:         (SELECT MAX(id) FROM frozen_t)                     |
|SQL Path:     spark_catalog.default, system.builtin              |
+-----------------------------------------------------------------+
```

### Related Statements

* [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
* [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
* [DESCRIBE QUERY](sql-ref-syntax-aux-describe-query.html)
* [CREATE FUNCTION (SQL)](sql-ref-syntax-ddl-create-sql-function.html)
* [Name Resolution](sql-ref-name-resolution.html)
