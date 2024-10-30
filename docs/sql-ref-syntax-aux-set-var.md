---
layout: global
title: SET VAR
displayTitle: SET VAR
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

The `SET VAR` command sets a temporary variable which has been previously declared in the current session.

To set a config variable or a hive variable use [SET](sql-ref-syntax-aux-conf-mgmt-set.html).

### Syntax

```sql
SET { VAR | VARIABLE }
  { { variable_name = { expression | DEFAULT } } [, ...] |
    ( variable_name [, ...] ) = ( query ) }
```

### Parameters

* **variable_name**

  Specifies an existing variable.
  If you specify multiple variables, there must not be any duplicates.

* **expression** 

  Any expression, including scalar subqueries.

* **DEFAULT**

  If you specify `DEFAULT`, the default expression of the variable is assigned,
  or `NULL` if there is none.

* **query**

  A [query](sql-ref-syntax-qry-select.html) that returns at most one row and as many columns as
  the number of specified variables. Each column must be implicitly castable to the data type of the
  corresponding variable.
  If the query returns no row `NULL` values are assigned.

### Examples

```sql
-- 
DECLARE VARIABLE var1 INT DEFAULT 7;
DECLARE VARIABLE var2 STRING;

-- A simple assignment
SET VAR var1 = 5;
SELECT var1;
  5

-- A complex expression assignment
SET VARIABLE var1 = (SELECT max(c1) FROM VALUES(1), (2) AS t(c1));
SELECT var1;
  2

-- resetting the variable to DEFAULT
SET VAR var1 = DEFAULT;
SELECT var1;
  7

-- A multi variable assignment
SET VAR (var1, var2) = (SELECT max(c1), CAST(min(c1) AS STRING) FROM VALUES(1), (2) AS t(c1));
SELECT var1, var2;
 2 1

-- Too many rows
SET VAR (var1, var2) = (SELECT c1, CAST(c1 AS STRING) FROM VALUES(1), (2) AS t(c1));
[ROW_SUBQUERY_TOO_MANY_ROWS] More than one row returned by a subquery used as a row. SQLSTATE: 21000

-- No rows
SET VAR (var1, var2) = (SELECT c1, CAST(c1 AS STRING) FROM VALUES(1), (2) AS t(c1) WHERE 1=0);
SELECT var1, var2;
  NULL NULL
```

### Related Statements

* [SET](sql-ref-syntax-aux-conf-mgmt-set.html)
