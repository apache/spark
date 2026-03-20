---
layout: global
title: CLOSE statement
displayTitle: CLOSE statement
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

Closes an open cursor and releases its resources.

The `CLOSE` statement closes a cursor that was previously opened with `OPEN`, freeing the memory and resources associated with its result set. After closing, the cursor can be reopened with `OPEN` to execute the query again with fresh parameter bindings.

## Syntax

```
CLOSE cursor_name
```

## Parameters

- **`cursor_name`**

  The name of an open cursor. The cursor can be optionally qualified with a compound statement label (e.g., `outer_label.my_cursor`).

## Examples

```SQL
-- Basic cursor lifecycle
> BEGIN
    DECLARE x INT;
    DECLARE my_cursor CURSOR FOR SELECT id FROM range(3);

    OPEN my_cursor;
    FETCH my_cursor INTO x;
    VALUES (x);
    CLOSE my_cursor;
  END;
0

-- Close cursor in handler
> BEGIN
    DECLARE x INT;
    DECLARE my_cursor CURSOR FOR SELECT id FROM range(2);

    DECLARE EXIT HANDLER FOR NOT FOUND
      BEGIN
        CLOSE my_cursor;
        VALUES ('Cursor closed on completion');
      END;

    OPEN my_cursor;
    REPEAT
      FETCH my_cursor INTO x;
    UNTIL false END REPEAT;
  END;
Cursor closed on completion

-- Reopen cursor with different parameters
> BEGIN
    DECLARE x INT;
    DECLARE param_cursor CURSOR FOR SELECT id FROM range(10) WHERE id = ?;

    OPEN param_cursor USING 3;
    FETCH param_cursor INTO x;
    VALUES ('First open:', x);
    CLOSE param_cursor;

    OPEN param_cursor USING 7;
    FETCH param_cursor INTO x;
    VALUES ('Second open:', x);
    CLOSE param_cursor;
  END;
First open:|3
Second open:|7

-- Qualified cursor name with label
> BEGIN
    outer_lbl: BEGIN
      DECLARE outer_cur CURSOR FOR SELECT id FROM range(3);
      DECLARE x INT;

      OPEN outer_cur;
      FETCH outer_cur INTO x;

      inner_lbl: BEGIN
        FETCH outer_lbl.outer_cur INTO x;
      END;

      CLOSE outer_lbl.outer_cur;
      VALUES ('Closed from outer scope');
    END;
  END;
Closed from outer scope

-- Processing all rows before close
> BEGIN
    DECLARE x INT;
    DECLARE done BOOLEAN DEFAULT false;
    DECLARE results STRING DEFAULT '';
    DECLARE my_cursor CURSOR FOR SELECT id FROM range(5);

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;

    OPEN my_cursor;
    REPEAT
      FETCH my_cursor INTO x;
      IF NOT done THEN
        SET results = results || CAST(x AS STRING) || ',';
      END IF;
    UNTIL done END REPEAT;
    CLOSE my_cursor;

    VALUES (results);
  END;
0,1,2,3,4,
```

## Notes

- The cursor must be open before calling `CLOSE`. Attempting to close a cursor that is not open raises a `CURSOR_NOT_OPEN` error.
- After closing, the cursor can be reopened with `OPEN`. This is useful when you want to re-execute the cursor's query with different parameter bindings.
- Cursors are automatically closed in the following scenarios:
  - When the compound statement that declares them exits normally
  - When an `EXIT` handler is triggered (all cursors in the compound statement and nested compounds are closed)
  - When the compound statement exits due to an unhandled exception
- It is good practice to explicitly close cursors when they are no longer needed, rather than relying on implicit closure. This frees resources earlier and makes the code's intent clearer.
- Closing a cursor does not affect the cursor declaration. The cursor name remains in scope and can be reopened.

## Related articles

- [Compound Statement](../control-flow/compound-stmt.html)
- [OPEN Statement](../control-flow/open-stmt.html)
- [FETCH Statement](../control-flow/fetch-stmt.html)
- [SQL Scripting](../sql-ref-scripting.html)
