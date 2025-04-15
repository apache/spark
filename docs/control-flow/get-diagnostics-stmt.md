---
layout: global
title: GET DIAGNOSTICS statement
displayTitle: GET DIAGNOSTICS statement
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

# GET DIAGNOSTICS statement

Retrieve information about a condition handled in an exception handler.

This statement may only be used within a condition handler in a [compound statement](compound-stmt.md).

## Syntax

```
GET DIAGNOSTICS CONDITION 1
  { variable_name = condition_info_item } [, ...]

condition_info_item
  { MESSAGE_TEXT |
    RETURNED_SQLSTATE |
    MESSAGE_ARGUMENTS |
    CONDITION_IDENTIFIER |
    LINE_NUMBER }
```

## Parameters

- **[variable_name](/sql/language-manual/sql-ref-names.md#variable-name)**

  A local variable or session variable.

- **`CONDITION 1`**

  Returns the condition that triggered the condition handler.
  You must call issue `GET DIAGNOSTICS CONDITION 1` as the first statement in the handler.

  - **`MESSAGE_TEXT`**

    Returns the message text associated with the condition as a `STRING`.
    `variable_name` must be a `STRING`.

  - **`RETURNED_SQLSTATE`**

    Returns the `SQLSTATE` associated with the condition being handled as a `STRING`.
    `variable_name` must be a `STRING`.

  - **`MESSAGE_ARGUMENTS`**

    Returns a `MAP<STRING, STRING>` mapping provided as arguments to the parameters of Databricks conditions.
    For declared conditions, the only map key is `MESSAGE_TEXT`.
    `variable_name` must be a `MAP<STRING, STRING>`

  - **`CONDITION_IDENTIFIER`**

    Returns the condition name that caused the exception.
    `variable_name` must be a `STRING`.

  - **`LINE_NUMBER`**

    Returns the line number of the statement raising the condition.
    `NULL` if not available.

## Examples

```SQL
-- Retrieve the number of rows inserted by an INSERT statement
> CREATE OR REPLACE TABLE emp(name STRING, salary DECIMAL(10, 2));

> BEGIN
    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
      BEGIN
        DECLARE cond STRING;
        DECLARE message STRING;
        DECLARE state STRING;
        DECLARE args MAP<STRING, STRING>;
        DECLARE line BIGINT;
        DECLARE argstr STRING;
        DECLARE log STRING;
        GET DIAGNOSTICS CONDITION 1
           cond    = CONDITION_IDENTIFIER,
           message = MESSAGE_TEXT,
           state   = RETURNED_SQLSTATE,
           args    = MESSAGE_ARGUMENTS,
           line    = LINE_NUMBER;
        SET argstr =
          (SELECT aggregate(array_agg('Parm:' || key || ' Val: value '),
                            '', (acc, x)->(acc || ' ' || x))
             FROM explode(args) AS args(key, val));
        SET log = 'Condition: ' || cond ||
                  ' Message: ' || message ||
                  ' SQLSTATE: ' || state ||
                  ' Args: ' || argstr ||
                  ' Line: ' || line;
        VALUES (log);
      END;
    SELECT 10/0;
  END;
 Condition: DIVIDE_BY_ZERO Message: Division by zero. Use try_divide to tolerate divisor being 0 and return NULL instead. If necessary, set <config> to “false” to bypass this error. SQLATTE: 22012 Args:  Parm: config Val: ANSI_MODE Line: 28
```

## Related articles

- [SQL Scripting](/sql/language-manual/sql-ref-scripting.md)
- [CASE Statement](/sql/language-manual/control-flow/case-stmt.md)
- [Compound Statement](/sql/language-manual/control-flow/compound-stmt.md)
- [FOR Statement](/sql/language-manual/control-flow/for-stmt.md)
