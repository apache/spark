---
layout: global
title: EXECUTE IMMEDIATE
displayTitle: EXECUTE IMMEDIATE
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

Executes a sql statement provided as a `STRING`, optionally passing `arg_exprN` to parameter markers and assigning the results to `var_nameN`.

### Syntax

```sql
EXECUTE IMMEDIATE sql_string
        [ INTO var_name [, …]  ]
        [ USING { (arg_expr [ AS ] [alias] [, …] ) | arg_expr [ AS ] [alias] [, …] } ]
```

### Parameters

* **sql_string**

  A STRING expression producing a well-formed SQL statement.

* **INTO var_name [, …]**

    Optionally returns the results of a single row query into SQL variables.
    If the query returns no rows the result is NULL.
    - `var_name`
    A SQL variable. A variable may not be referenced more than once.

* **USING arg_expr [, …]**

  Optionally, if sql_string contains parameter markers, binds in values to the parameters.
  - `arg_expr`
  An expression that binds to a parameter marker.
  If the parameter markers are unnamed the binding is by position.
  For unnamed parameter markers, binding is by name.
  - `alias`
    Overrides the name used to bind `arg_expr` to a named parameter marker

  Each named parameter marker must be matched once. Not all arg_expr must be matched.


### Examples

```sql
-- A self-contained execution using a literal string
EXECUTE IMMEDIATE 'SELECT SUM(col1) FROM VALUES(?), (?)' USING 5, 6;
 11

-- A SQL string composed in a SQL variable
DECLARE sqlStr = 'SELECT SUM(col1) FROM VALUES(?), (?)';
DECLARE arg1 = 5;
DECLARE arg2 = 6;
EXECUTE IMMEDIATE sqlStr USING arg1, arg2;
 11

-- Using the INTO clause
DECLARE sum INT;
EXECUTE IMMEDIATE sqlStr INTO sum USING arg1, arg2;
SELECT sum;
 11

-- Using named parameter markers
SET VAR sqlStr = 'SELECT SUM(col1) FROM VALUES(:first), (:second)';
EXECUTE IMMEDIATE sqlStr INTO sum USING 5 AS first, arg2 AS second;
SELECT sum;
 11
```