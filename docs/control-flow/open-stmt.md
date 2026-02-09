---
layout: global
title: OPEN statement
displayTitle: OPEN statement
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

Opens a cursor and executes its query, positioning it before the first row.

The `OPEN` statement executes the query defined in the cursor declaration, binding any parameter markers if specified. Once opened, the cursor can be used with `FETCH` to retrieve rows.

## Syntax

```
OPEN cursor_name [ USING { constant_expr [ AS param_name ] } [, ...] ]
```

## Parameters

- **`cursor_name`**

  The name of a declared cursor. The cursor can be optionally qualified with a compound statement label to reference a cursor from an outer scope (e.g., `outer_label.my_cursor`).

- **`USING { constant_expr [ AS param_name ] } [, ...]`**

  Optional clause to bind values to parameter markers in the cursor's query.

  - **`constant_expr`**

    A constant expression (literal or variable) to bind to a parameter marker. The expression must be castable to the type expected by the query.

  - **`AS param_name`**

    An optional alias for the parameter. For named parameter markers (`:param_name`), this specifies which parameter to bind. If not specified for positional markers (`?`), parameters are bound by position.

## Examples

```SQL
-- Open a simple cursor without parameters
> BEGIN
    DECLARE total INT;
    DECLARE my_cursor CURSOR FOR SELECT sum(id) FROM range(10);

    OPEN my_cursor;
    FETCH my_cursor INTO total;
    VALUES (total);
    CLOSE my_cursor;
  END;
45

-- Open cursor with positional parameters
> BEGIN
    DECLARE total INT;
    DECLARE param_cursor CURSOR FOR
      SELECT sum(id) FROM range(100) WHERE id BETWEEN ? AND ?;

    OPEN param_cursor USING 10, 20;
    FETCH param_cursor INTO total;
    VALUES (total);
    CLOSE param_cursor;
  END;
165

-- Open cursor with named parameters
> BEGIN
    DECLARE min_val INT;
    DECLARE named_cursor CURSOR FOR
      SELECT min(id) FROM range(100) WHERE id >= :threshold;

    OPEN named_cursor USING 25 AS threshold;
    FETCH named_cursor INTO min_val;
    VALUES (min_val);
    CLOSE named_cursor;
  END;
25

-- Open cursor using variables as parameters
> BEGIN
    DECLARE lower INT DEFAULT 5;
    DECLARE upper INT DEFAULT 15;
    DECLARE result INT;
    DECLARE var_cursor CURSOR FOR
      SELECT count(*) FROM range(100) WHERE id BETWEEN ? AND ?;

    OPEN var_cursor USING lower, upper;
    FETCH var_cursor INTO result;
    VALUES (result);
    CLOSE var_cursor;
  END;
11

-- Open cursor with various data types
> BEGIN
    DECLARE type_name STRING;
    DECLARE value_sum INT;
    DECLARE type_cursor CURSOR FOR
      SELECT typeof(:p) as type, sum(:p + id) FROM range(3);

    OPEN type_cursor USING 10 AS p;
    FETCH type_cursor INTO type_name, value_sum;
    VALUES (type_name, value_sum);
    CLOSE type_cursor;
  END;
INT|33

-- Qualified cursor name with label
> BEGIN
    outer_lbl: BEGIN
      DECLARE outer_cur CURSOR FOR SELECT max(id) FROM range(10);
      DECLARE max_val INT;

      OPEN outer_cur;

      inner_lbl: BEGIN
        DECLARE inner_cur CURSOR FOR SELECT min(id) FROM range(5);
        DECLARE min_val INT;

        OPEN inner_cur;
        FETCH outer_lbl.outer_cur INTO max_val;
        FETCH inner_cur INTO min_val;
        VALUES (max_val, min_val);
        CLOSE inner_cur;
      END;

      CLOSE outer_cur;
    END;
  END;
9|0
```

## Notes

- A cursor can only be opened once. Attempting to open an already-opened cursor raises a `CURSOR_ALREADY_OPEN` error.
- Parameter binding behavior matches `EXECUTE IMMEDIATE`:
  - For positional parameters (`?`), expressions are bound in the order specified.
  - For named parameters (`:name`), the `AS param_name` clause specifies the binding.
  - All parameter markers in the query must be bound.
- Variable references in the cursor's query are evaluated at `OPEN` time, using current variable values.
- The cursor's result set is materialized at `OPEN` time. Subsequent changes to variables or tables do not affect the result set.
- If the cursor's query raises an error during execution, the cursor remains in a closed state.

## Related articles

- [Compound Statement](../control-flow/compound-stmt.html)
- [FETCH Statement](../control-flow/fetch-stmt.html)
- [CLOSE Statement](../control-flow/close-stmt.html)
- [EXECUTE IMMEDIATE Statement](../sql-ref-syntax-aux-exec-imm.html)
- [SQL Scripting](../sql-ref-scripting.html)
