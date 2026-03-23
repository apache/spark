---
layout: global
title: FETCH statement
displayTitle: FETCH statement
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

Fetches the next row from an open cursor into variables.

The `FETCH` statement retrieves one row at a time from the cursor's result set and assigns column values to the specified variables. If no more rows are available, the `CURSOR_NO_MORE_ROWS` condition is raised (SQLSTATE `'02000'`).

## Syntax

```
FETCH [ [ NEXT ] FROM ] cursor_name INTO variable_name [, ...]
```

## Parameters

- **`cursor_name`**

  The name of an open cursor. The cursor can be optionally qualified with a compound statement label (e.g., `outer_label.my_cursor`).

- **`NEXT FROM`**

  Optional keywords. `NEXT` and `FROM` are syntactic sugar and do not affect behavior. Only forward fetching is supported.

- **`variable_name`**

  A local or session variable to receive column values. The number of variables must match the number of columns in the cursor's result set, with one exception:
  - If exactly one variable is specified and it is a `STRUCT` type, and the cursor returns multiple columns, the column values are assigned to the struct's fields by position.

  Column data types must be compatible with the target variables (or struct fields) according to store assignment rules.

## Examples

```SQL
-- Basic fetch into variables
> BEGIN
    DECLARE x INT;
    DECLARE y STRING;
    DECLARE my_cursor CURSOR FOR
      SELECT id, 'row_' || id FROM range(3);

    OPEN my_cursor;
    FETCH my_cursor INTO x, y;
    VALUES (x, y);
    CLOSE my_cursor;
  END;
0|row_0

-- Fetch multiple rows with REPEAT loop
> BEGIN
    DECLARE x INT;
    DECLARE done BOOLEAN DEFAULT false;
    DECLARE total INT DEFAULT 0;
    DECLARE sum_cursor CURSOR FOR SELECT id FROM range(5);

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;

    OPEN sum_cursor;
    REPEAT
      FETCH sum_cursor INTO x;
      IF NOT done THEN
        SET total = total + x;
      END IF;
    UNTIL done END REPEAT;
    CLOSE sum_cursor;

    VALUES (total);
  END;
10

-- Fetch into a struct variable
> BEGIN
    DECLARE result STRUCT<id: INT, name: STRING>;
    DECLARE struct_cursor CURSOR FOR
      SELECT id, 'name_' || id FROM range(3);

    OPEN struct_cursor;
    FETCH struct_cursor INTO result;
    VALUES (result.id, result.name);
    CLOSE struct_cursor;
  END;
0|name_0

-- Using NEXT FROM (optional syntax)
> BEGIN
    DECLARE x INT;
    DECLARE cursor1 CURSOR FOR SELECT id FROM range(3);

    OPEN cursor1;
    FETCH NEXT FROM cursor1 INTO x;
    VALUES (x);
    CLOSE cursor1;
  END;
0

-- Qualified cursor name with label
> BEGIN
    outer_lbl: BEGIN
      DECLARE outer_cur CURSOR FOR SELECT id FROM range(5);
      DECLARE x INT;

      OPEN outer_cur;

      inner_lbl: BEGIN
        FETCH outer_lbl.outer_cur INTO x;
        VALUES (x);
      END;

      CLOSE outer_cur;
    END;
  END;
0

-- Exit handler for NOT FOUND
> BEGIN
    DECLARE x INT;
    DECLARE my_cursor CURSOR FOR SELECT id FROM range(2);

    DECLARE EXIT HANDLER FOR NOT FOUND
      BEGIN
        VALUES ('No more rows');
      END;

    OPEN my_cursor;
    FETCH my_cursor INTO x;
    FETCH my_cursor INTO x;
    FETCH my_cursor INTO x; -- Triggers EXIT handler
    VALUES ('This will not execute');
    CLOSE my_cursor;
  END;
No more rows

-- Specific CURSOR_NO_MORE_ROWS handler
> BEGIN
    DECLARE x INT DEFAULT 0;
    DECLARE done BOOLEAN DEFAULT false;
    DECLARE count INT DEFAULT 0;
    DECLARE my_cursor CURSOR FOR SELECT id FROM range(3);

    DECLARE CONTINUE HANDLER FOR CURSOR_NO_MORE_ROWS SET done = true;

    OPEN my_cursor;
    WHILE NOT done DO
      FETCH my_cursor INTO x;
      IF NOT done THEN
        SET count = count + 1;
      END IF;
    END WHILE;
    CLOSE my_cursor;

    VALUES (count);
  END;
3
```

## Notes

- The cursor must be opened with `OPEN` before calling `FETCH`. Attempting to fetch from a closed cursor raises a `CURSOR_NOT_OPEN` error.
- Each `FETCH` advances the cursor position by one row.
- When no more rows are available, `FETCH` raises the `CURSOR_NO_MORE_ROWS` condition:
  - SQLSTATE: `'02000'`
  - Error condition: `CURSOR_NO_MORE_ROWS`
  - This is caught by `NOT FOUND` handlers (which catch all SQLSTATE `'02xxx'` conditions)
- If no `CONTINUE HANDLER` or `EXIT HANDLER` is declared for `NOT FOUND`, the completion condition is silently ignored and execution continues. This allows scripts to continue after exhausting a cursor.
- Type compatibility follows store assignment rules:
  - Implicit casts are applied when possible
  - Incompatible types raise a type mismatch error
- Variables can be local variables declared in the compound statement or session variables created with `DECLARE VARIABLE` at the session level.

## Related articles

- [Compound Statement](../control-flow/compound-stmt.html)
- [OPEN Statement](../control-flow/open-stmt.html)
- [CLOSE Statement](../control-flow/close-stmt.html)
- [WHILE Statement](../control-flow/while-stmt.html)
- [REPEAT Statement](../control-flow/repeat-stmt.html)
- [SQL Scripting](../sql-ref-scripting.html)
