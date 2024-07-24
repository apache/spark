---
layout: global
title: DECLARE VARIABLE
displayTitle: DECLARE VARIABLE
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

The `DECLARE VARIABLE` statement is used to create a temporary variable in Spark.
Temporary variables are scoped at a session level.

You can reference variables by their name everywhere constant expressions are allowed.
Unless you qualify a variable with `session` or `system.session`, a variable is only resolved after
Spark fails to resolve a name to a column or column alias.

Temporary variables cannot be referenced in persisted objects such as persisted view,
column default expressions, and generated column expressions.

### Syntax

```sql
DECLARE [ OR REPLACE ] [ VAR | VARIABLE ]
    variable_name [ data_type ] [ { DEFAULT | = } default_expr ]
```

### Parameters

* **OR REPLACE**

    If specified, a pre-existing temporary variable is replaced if it exists.

* **variable_name**

    Specifies a name for the variable to be created.
    The variable name may be optionally qualified with a `system`.`session` or `session`.

    **Syntax:** `[ system . [ session .] ] variable_name`

* **data_type**

    Optionally defines the data type of the variable.
    If it is not specified the type is derived from the default expression.

* **default_expr**

    An optional expression used to initialize the value of the variable after declaration.
    The expression is re-evaluated whenever the variable is reset to `DEFAULT` using
    [SET VAR](sql-ref-syntax-aux-set-var.html).
    If `data_type` is specified `default_expr` must be castable to the variable type.
    If `data_type` is not specified you must specify a default and its type will become the type of
    the variable.
    If no default expression is given, the variable is initialized with `NULL`.

### Examples

```sql
-- The dense form of declaring a variable with default
DECLARE five = 5;

-- Declare a defined variable
DECLARE five = 55;
[VARIABLE_ALREADY_EXISTS] Cannot create the variable `system`.`session`.`five` because it already exists.
Choose a different name, or drop or replace the existing variable. SQLSTATE: 42723

-- Use `DECLARE OR REPLACE` to declare a defined variable
DECLARE OR REPLACE five = 55;

-- Explicitly declare the default value of a variable using the keyword `DEFAULT`
DECLARE VARIABLE size DEFAULT 6;

-- STRING variable initialialized to `NULL`
DECLARE some_var STRING;
```

### Related Statements

* [DROP TEMPORARY VARIABLE](sql-ref-syntax-ddl-drop-variable.html)
* [SET VARIABLE](sql-ref-syntax-aux-set-var.html)
