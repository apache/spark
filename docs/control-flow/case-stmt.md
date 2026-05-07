---
layout: global
title: CASE statement
displayTitle: CASE statement
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

Executes `thenStmtN` for the first `optN` that equals `expr` or `elseStmt` if no `optN` matches `expr`.
This is called a _simple case statement_.

Executes `thenStmtN` for the first `condN` evaluating to `true`, or `elseStmt` if no `condN` evaluates to `true`.
This is called a _searched case statement_.

For case expressions that yield result values, see `CASE expression`)

This statement may only be used within a [compound statement](compound-stmt.html).

## Syntax

```
CASE expr
  { WHEN opt THEN { thenStmt ; } [...] } [...]
  [ ELSE { elseStmt ; } [...] ]
END CASE

CASE
  { WHEN cond THEN { thenStmt ; } [...] } [...]
  [ ELSE { elseStmt ; } [...] ]
END CASE
```

## Parameters

- **`expr`**: Any expression for which a comparison is defined.
- **`opt`**: An expression with a least common type with `expr` and all other `optN`.
- **`thenStmt`**: A SQL Statement to execute if preceding condition is `true`.
- **`elseStmt`**: A SQL Statement to execute if no condition is `true`.
- **`cond`**: A `BOOLEAN` expression.

Conditions are evaluated in order, and only the first set of `stmt` for which `opt` or `cond` evaluate to true will be executed.

## Examples

```SQL
-- a simple case statement
> BEGIN
    DECLARE choice INT DEFAULT 3;
    DECLARE result STRING;
    CASE choice
      WHEN 1 THEN
        VALUES ('one fish');
      WHEN 2 THEN
        VALUES ('two fish');
      WHEN 3 THEN
        VALUES ('red fish');
      WHEN 4 THEN
        VALUES ('blue fish');
      ELSE
        VALUES ('no fish');
    END CASE;
  END;
 red fish

-- A searched case statement
> BEGIN
    DECLARE choice DOUBLE DEFAULT 3.9;
    DECLARE result STRING;
    CASE
      WHEN choice < 2 THEN
        VALUES ('one fish');
      WHEN choice < 3 THEN
        VALUES ('two fish');
      WHEN choice < 4 THEN
        VALUES ('red fish');
      WHEN choice < 5 OR choice IS NULL THEN
        VALUES ('blue fish');
      ELSE
        VALUES ('no fish');
    END CASE;
  END;
 red fish
```

## Related articles

- [SQL Scripting](../sql-ref-scripting.html)
- [compound statement](compound-stmt.html)
- [IF statement](if-stmt.html)
