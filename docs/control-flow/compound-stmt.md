---
layout: global
title: compound statement
displayTitle: compound statement
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

Implements a SQL Script block that can contain a sequence of SQL statements, control-of-flow statements, local variable declarations, and exception handlers.

## Syntax

```
[ label : ]
      BEGIN
      [ { declare_variable | declare_condition } ; [...] ]
      [ declare_cursor ; [...] ]
      [ declare_handler ; [...] ]
      [ SQL_statement ; [...] ]
      END [ label ]

declare_variable
  DECLARE variable_name [, ...] datatype [ DEFAULT default_expr ]

declare_condition
  DECLARE condition_name CONDITION [ FOR SQLSTATE [ VALUE ] sqlstate ]

declare_cursor
  DECLARE cursor_name [ ASENSITIVE | INSENSITIVE ] CURSOR
    FOR query

declare_handler
  DECLARE handler_type HANDLER FOR condition_values handler_action

handler_type
  { EXIT | CONTINUE }

condition_values
 { { SQLSTATE [ VALUE ] sqlstate | condition_name } [, ...] |
   { SQLEXCEPTION | NOT FOUND } [, ...] }
```

## Parameters

- **`label`**

  An optional identifier is used to qualify variables defined within the compound and to leave the compound.
  Both label occurrences must match, and the `END` label can only be specified if `label:` is specified.

  `label` must not be specified for a top level compound statement.

- **`NOT ATOMIC`**

  Specifies that, if an SQL statement within the compound fails, previous SQL statements will not be rolled back.
  This is the default and only behavior.

- **`declare_variable`**

  A local variable declaration for one or more variables

  - **`variable_name`**

    A name for the variable.
    The name must not be qualified, and be unique within the compound statement.

  - **`data_type`**

    Any supported data type. If data_type is omitted, you must specify DEFAULT, and the type is derived from the default_expression.

  - **`{ DEFAULT | = } default_expression`**

    Defines the variable's initial value after declaration. default_expression must be castable to data_type. If no default is specified, the variable is initialized with NULL.

- **`Declare_condition`**

  A local condition declaration

  - **`condition_name`**

    The unqualified name of the condition is scoped to the compound statement.

  - **`sqlstate`**

    A `STRING` literal of 5 alphanumeric characters (case insensitive) consisting of A-Z and 0..9. The SQLSTATE must not start with `'00'`, `'01'`, or `'XX'`. Any SQLSTATE starting with `'02'` will be caught by the predefined `NOT FOUND` handler as well. If not specified, the SQLSTATE is `'45000'`.

- **`declare_cursor`**

  A local cursor declaration for iterating through query results.

  - **`cursor_name`**

    An unqualified name for the cursor. The name must be unique among all cursors declared in this compound statement. Cursors can be qualified with the compound statement `label` to disambiguate duplicate names.

  - **`ASENSITIVE`** or **`INSENSITIVE`**

    Optional keywords specifying that once the cursor is opened, the result set is not affected by DML changes within or outside the session. This is the default and only supported behavior.

  - **`query`**

    The query that defines the cursor. The query is not executed until the cursor is opened with `OPEN cursor_name`. At open time, any variable references and parameter markers in the query are bound to their current values.

- **`declare_handler`**

  A declaration for an error handler.

  - **`handler_type`**

    - **`EXIT`**

      Classifies the handler to exit the compound statement after the condition is handled. All cursors opened within the compound statement and nested compound statements are implicitly closed.

    - **`CONTINUE`**

      Classifies the handler to continue execution after the handler completes. Execution resumes with the statement following the one that raised the condition.

  - **`condition_values`**

    Specifies to which sqlstates or conditions the handler applies.
    Condition values must be unique within all handlers within the compound statement.
    Specific condition values take precedence over `SQLEXCEPTION`.

  - **`sqlstate`**

    A `STRING` literal of 5 characters `'A'-'Z'` and `'0'-'9'` (case insensitive).

  - **`condition_name`**

    A condition defined within this compound, an outer compound statement, or a system-defined error class.

  - **`SQLEXCEPTION`**

    Applies to any user-facing error condition.

  - **`NOT FOUND`**

    Applies to any condition with a SQLSTATE `'02xxx'` class. This includes the `CURSOR_NO_MORE_ROWS` condition (SQLSTATE `'02000'`) raised when fetching beyond the end of a cursor's result set.

  - **`handler_action`**

    A SQL statement to execute when any of the condition values occur.
    To add multiple statements, use a nested compound statement.

- **`SQL_statement`**

  A SQL statement such as a DDL, DML, control statement, or compound statement.
  Any `SELECT` or `VALUES` statement produces a result set that the invoker can consume.

## Examples

```SQL
-- A compound statement with local variables, an exit handler and a nested compound.
> BEGIN
    DECLARE a INT DEFAULT 1;
    DECLARE b INT DEFAULT 5;
    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
      div0: BEGIN
        VALUES (15);
      END div0;
    SET a = 10;
    SET a = b / 0;
    VALUES (a);
END;
15

-- A compound statement with a cursor and a CONTINUE handler for iteration.
> BEGIN
    DECLARE x INT;
    DECLARE done BOOLEAN DEFAULT false;
    DECLARE total INT DEFAULT 0;
    DECLARE my_cursor CURSOR FOR SELECT id FROM range(5);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;

    OPEN my_cursor;
    REPEAT
      FETCH my_cursor INTO x;
      IF NOT done THEN
        SET total = total + x;
      END IF;
    UNTIL done END REPEAT;
    CLOSE my_cursor;

    VALUES (total);
  END;
10
```

## Related articles

- [SQL Scripting](../sql-ref-scripting.html)
- [OPEN Statement](../control-flow/open-stmt.html)
- [FETCH Statement](../control-flow/fetch-stmt.html)
- [CLOSE Statement](../control-flow/close-stmt.html)
- [CASE Statement](../control-flow/case-stmt.html)
- [IF Statement](../control-flow/if-stmt.html)
- [LOOP Statement](../control-flow/loop-stmt.html)
- [WHILE Statement](../control-flow/while-stmt.html)
- [REPEAT Statement](../control-flow/repeat-stmt.html)
- [FOR Statement](../control-flow/for-stmt.html)
- [ITERATE Statement](../control-flow/iterate-stmt.html)
- [LEAVE Statement](../control-flow/leave-stmt.html)
