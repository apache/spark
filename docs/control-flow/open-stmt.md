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
    DECLARE x INT;
    DECLARE my_cursor CURSOR FOR SELECT id FROM range(3);
    
    OPEN my_cursor;
    FETCH my_cursor INTO x;
    VALUES (x);
    CLOSE my_cursor;
  END;
0

-- Open cursor with positional parameters
> BEGIN
    DECLARE x INT;
    DECLARE param_cursor CURSOR FOR 
      SELECT id FROM range(10) WHERE id BETWEEN ? AND ?;
    
    OPEN param_cursor USING 3, 7;
    FETCH param_cursor INTO x;
    VALUES (x);
    CLOSE param_cursor;
  END;
3

-- Open cursor with named parameters
> BEGIN
    DECLARE result INT;
    DECLARE named_cursor CURSOR FOR 
      SELECT id FROM range(100) WHERE id >= :min_val AND id <= :max_val;
    
    OPEN named_cursor USING 10 AS min_val, 20 AS max_val;
    FETCH named_cursor INTO result;
    VALUES (result);
    CLOSE named_cursor;
  END;
10

-- Open cursor using variables as parameters
> BEGIN
    DECLARE min_id INT DEFAULT 5;
    DECLARE max_id INT DEFAULT 8;
    DECLARE result INT;
    DECLARE var_cursor CURSOR FOR SELECT id FROM range(10) WHERE id BETWEEN ? AND ?;
    
    OPEN var_cursor USING min_id, max_id;
    FETCH var_cursor INTO result;
    VALUES (result);
    CLOSE var_cursor;
  END;
5

-- Open cursor with various data types
> BEGIN
    DECLARE str_val STRING;
    DECLARE int_val INT;
    DECLARE type_cursor CURSOR FOR SELECT typeof(:p) as type, :p as val;
    
    OPEN type_cursor USING 42 AS p;
    FETCH type_cursor INTO str_val, int_val;
    CLOSE type_cursor;
    
    VALUES ('INT', int_val);
  END;
INT|42

-- Qualified cursor name with label
> BEGIN
    outer_lbl: BEGIN
      DECLARE outer_cur CURSOR FOR SELECT id FROM range(3);
      OPEN outer_cur;
      
      inner_lbl: BEGIN
        DECLARE inner_cur CURSOR FOR SELECT id * 10 FROM range(2);
        DECLARE x INT;
        
        OPEN inner_cur;
        FETCH outer_lbl.outer_cur INTO x;
        VALUES (x);
        CLOSE inner_cur;
      END;
      
      CLOSE outer_cur;
    END;
  END;
0
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
- [DECLARE CURSOR Statement](../control-flow/declare-cursor-stmt.html)
- [FETCH Statement](../control-flow/fetch-stmt.html)
- [CLOSE Statement](../control-flow/close-stmt.html)
- [EXECUTE IMMEDIATE Statement](../sql-ref-syntax-aux-exec-imm.html)
- [SQL Scripting](../sql-ref-scripting.html)
