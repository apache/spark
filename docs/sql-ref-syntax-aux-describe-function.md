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
existing function. The metadata information includes the function name, implementing
class and the usage details.  If the optional `EXTENDED` option is specified, the basic
metadata information is returned along with the extended usage information.

### Syntax

```sql
{ DESC | DESCRIBE } FUNCTION [ EXTENDED ] function_name
```

### Parameters

* **function_name**

    Specifies a name of an existing function in the system. The function name may be
    optionally qualified with a database name. If `function_name` is qualified with
    a database then the function is resolved from the user specified database, otherwise
    it is resolved from the current database.

    **Syntax:** `[ database_name. ] function_name`

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
DESC FUNCTION EXTENDED explode
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
```

### Related Statements

* [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
* [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
* [DESCRIBE QUERY](sql-ref-syntax-aux-describe-query.html)
