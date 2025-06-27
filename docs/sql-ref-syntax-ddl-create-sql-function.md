---
layout: global
title: CREATE FUNCTION (SQL)
displayTitle: CREATE FUNCTION (SQL)
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
The `CREATE FUNCTION` statement creates a SQL function that can be used in SQL statements. The function can be temporary or permanent, and can return either a scalar value or a table result. The function body can be defined either a SQL expression or a query.

When `TEMPORARY` is specified, the function is only available for the current session. Otherwise, it is persisted in the catalog and available across sessions. The `OR REPLACE` option allows updating an existing function definition, while `IF NOT EXISTS` prevents errors when creating a function that already exists.

The function parameters must be specified with their data types. The return type can be either a scalar data type or a table with an optional schema definition. 


### Syntax

```sql
CREATE [OR REPLACE] [TEMPORARY] FUNCTION [IF NOT EXISTS]
    function_name ( [ function_parameter [, ...] ] )
    { [ RETURNS data_type ] |
      RETURNS TABLE [ ( column_spec [, ...]) ] }
    [ characteristic [...] ]
    RETURN { expression | query }

function_parameter
    parameter_name data_type [DEFAULT default_expression] [COMMENT parameter_comment]

column_spec
    column_name data_type [COMMENT column_comment]

characteristic
  { LANGUAGE SQL |
    [NOT] DETERMINISTIC |
    COMMENT function_comment |
    [CONTAINS SQL | READS SQL DATA] }
```

### Parameters

- **OR REPLACE**

  If specified, the function with the same name and signature (number of parameters and parameter types) is replaced. You cannot replace an existing function with a different signature or a procedure. This is mainly useful to update the function body and the return type of the function. You cannot specify this parameter with `IF NOT EXISTS`.

- **TEMPORARY**

  The scope of the function being created. When you specify `TEMPORARY`, the created function is valid and visible in the current session. No persistent entry is made in the catalog.

- **IF NOT EXISTS**

  If specified, creates the function only when it does not exist. The creation of the function succeeds (no error is thrown) if the specified function already exists in the system. You cannot specify this parameter with `OR REPLACE`.

- **function_name**

  A name for the function. For a permanent function, you can optionally qualify the function name, or it will be created under the current catalog and namespace.
  If the name is not qualified the permanent function is created in the current schema.

  **Syntax:** `[ database_name. ] function_name`

- **function_parameter**

  Specifies a parameter of the function.

  - **[parameter_name](sql-ref-identifier.md)**

    The parameter name must be unique within the function.

  - **[data_type](sql-ref-datatypes.md)**

    Any supported data type.

  - **DEFAULT default_expression**

    An optional default to be used when a function invocation does not assign an argument to the parameter.
    `default_expression` must be castable to `data_type`.
    The expression must not reference another parameter or contain a subquery.

    When you specify a default for one parameter, all following parameters must also have a default.

  - **COMMENT comment**

    An optional description of the parameter. `comment` must be a `STRING` literal.

- **RETURNS [data_type](sql-ref-datatypes.md)**

  The return data type of the scalar function. This clause is optional. The data type will be derived from the SQL function body if it is not provided.

- **RETURNS TABLE [ (column_spec [,…] ) ]**

  This clause marks the function as a table function.
  Optionally it also specifies the signature of the result of the table function.
  If no column_spec is specified it will be derived from the body of the SQL UDF.

  - **[column_name](sql-ref-identifier.md)**

    The column name must be unique within the signature.

  - **[data_type](sql-ref-datatypes.md)**

    Any supported data type.

  - **COMMENT column_comment**

    An optional description of the column. `comment` must be a `STRING` literal.

- **RETURN \{ expression| query \}**

  The body of the function. For a scalar function, it can either be a query or an expression. For a table function, it can only be a query. The expression cannot contain:

  - [Aggregate functions](sql-ref-functions-builtin.md#aggregate-functions)
  - [Window functions](sql-ref-functions-builtin.md#analytic-window-functions)
  - [Ranking functions](sql-ref-functions-builtin.md#ranking-window-functions)
  - Row producing functions such as `explode`

  Within the body of the function you can refer to parameter by its unqualified name or by qualifying the parameter with the function name.

- **characteristic**

  All characteristic clauses are optional.
  You can specify any number of them in any order, but you can specify each clause only once.

  - **LANGUAGE SQL**

    The language of the function implementation.

  - **[NOT] DETERMINISTIC**

    Whether the function is deterministic.
    A function is deterministic when it returns only one result for a given set of arguments.
    You may mark a function as `DETERMINISTIC` when its body is not and vice versa.
    A reason for this may be to encourage or discourage query optimizations such as constant
    folding or query caching.
    If you do not specify ths option it is derived from the function body.

  - **COMMENT function_comment**

    A comment for the function. `function_comment` must be String literal.

  - **CONTAINS SQL** or **READS SQL DATA**

    Whether a function reads data directly or indirectly from a table or a view.
    When the function reads SQL data, you cannot specify `CONTAINS SQL`.
    If you don't specify either clause, the property is derived from the function body.

## Examples


### Create and use a SQL scalar function

```sql
> CREATE VIEW t(c1, c2) AS VALUES (0, 1), (1, 2);

-- Create a temporary function with no parameter.
> CREATE TEMPORARY FUNCTION hello() RETURNS STRING
    RETURN 'Hello World!';

> SELECT hello();
  Hello World!

-- Create a permanent function with parameters.
> CREATE FUNCTION area(x DOUBLE, y DOUBLE) RETURNS DOUBLE RETURN x * y;

-- Use a SQL function in the SELECT clause of a query.
> SELECT area(c1, c2) AS area FROM t;
 1.0
 1.0

-- Use a SQL function in the WHERE clause of a query.
> SELECT * FROM t WHERE area(c1, c2) > 0;
 1  2

-- Compose SQL functions.
> CREATE FUNCTION square(x DOUBLE) RETURNS DOUBLE RETURN area(x, x);

> SELECT c1, square(c1) AS square FROM t;
  0  0.0
  1  1.0

-- Create a non-deterministic function
> CREATE FUNCTION roll_dice()
    RETURNS INT
    NOT DETERMINISTIC
    CONTAINS SQL
    COMMENT 'Roll a single 6 sided die'
    RETURN (rand() * 6)::INT + 1;
-- Roll a single 6-sided die
> SELECT roll_dice();
 3
```

### Create a SQL table function

```sql
-- Produce all weekdays between two dates
> CREATE FUNCTION weekdays(start DATE, end DATE)
    RETURNS TABLE(day_of_week STRING, day DATE)
    RETURN SELECT extract(DAYOFWEEK_ISO FROM day), day
             FROM (SELECT sequence(weekdays.start, weekdays.end)) AS T(days)
                  LATERAL VIEW explode(days) AS day
             WHERE extract(DAYOFWEEK_ISO FROM day) BETWEEN 1 AND 5;

-- Return all weekdays
> SELECT weekdays.day_of_week, day
    FROM weekdays(DATE'2022-01-01', DATE'2022-01-14');
  1     2022-01-03
  2     2022-01-04
  3     2022-01-05
  4     2022-01-06
  5     2022-01-07
  1     2022-01-10
  2     2022-01-11
  3     2022-01-12
  4     2022-01-13
  5     2022-01-14

-- Return weekdays for date ranges originating from a LATERAL correlation
> SELECT weekdays.*
    FROM VALUES (DATE'2020-01-01'),
                (DATE'2021-01-01'),
                (DATE'2022-01-01') AS starts(start),
         LATERAL weekdays(start, start + INTERVAL '7' DAYS);
  3     2020-01-01
  4     2020-01-02
  5     2020-01-03
  1     2020-01-06
  2     2020-01-07
  3     2020-01-08
  5     2021-01-01
  1     2021-01-04
  2     2021-01-05
  3     2021-01-06
  4     2021-01-07
  5     2021-01-08
  1     2022-01-03
  2     2022-01-04
  3     2022-01-05
  4     2022-01-06
  5     2022-01-07
```

### Replace a SQL function

```sql
-- Replace a SQL scalar function.
> CREATE OR REPLACE FUNCTION square(x DOUBLE) RETURNS DOUBLE RETURN x * x;

-- Replace a SQL table function.
> CREATE OR REPLACE FUNCTION getemps(deptno INT)
    RETURNS TABLE (name STRING)
    RETURN SELECT name FROM employee e WHERE e.deptno = getemps.deptno;

-- Describe a SQL table function.
> DESCRIBE FUNCTION getemps;
 Function: default.getemps
 Type:     TABLE
 Input:    deptno INT
 Returns:  id   INT
           name STRING
```

### Describe a SQL function

```sql
> DESCRIBE FUNCTION hello;
 Function: hello
 Type:     SCALAR
 Input:    ()
 Returns:  STRING

> DESCRIBE FUNCTION area;
 Function: default.area
 Type:     SCALAR
 Input:    x DOUBLE
           y DOUBLE
 Returns:  DOUBLE

> DESCRIBE FUNCTION roll_dice;
 Function: default.roll_dice
 Type:     SCALAR
 Input:    num_dice  INT
           num_sides INT
 Returns:  INT
```

### Related Statements

* [SHOW FUNCTIONS](sql-ref-syntax-aux-show-functions.html)
* [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
* [DROP FUNCTION](sql-ref-syntax-ddl-drop-function.html)
