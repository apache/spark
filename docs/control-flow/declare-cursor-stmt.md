---
layout: global
title: DECLARE CURSOR statement
displayTitle: DECLARE CURSOR statement
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

Declares a cursor for iterating through the results of a query within a SQL script.

A cursor allows procedural processing of query results one row at a time. The cursor is declared with a query, opened to execute the query, fetched from to retrieve rows, and finally closed to release resources.

## Syntax

```
DECLARE cursor_name [ ASENSITIVE | INSENSITIVE ] CURSOR
  FOR query
```

## Parameters

- **`cursor_name`**

  An unqualified name for the cursor. The name must be unique among all cursors declared in the compound statement. Cursors can be qualified with the compound statement label to disambiguate duplicate names in nested scopes.

- **`ASENSITIVE`** or **`INSENSITIVE`**

  Optional keywords specifying the cursor's sensitivity to changes in the underlying data. Both specify that once the cursor is opened, the result set is not affected by DML changes. This is the default and currently the only supported behavior.

- **`query`**

  The `SELECT` or `VALUES` query that defines the cursor's result set. The query can include:
  - Variable references that are bound when the cursor is opened
  - Parameter markers (`?` or `:name`) that are bound via the `OPEN` statement's `USING` clause

  The query is not executed until `OPEN cursor_name` is called.

## Examples

```SQL
-- Basic cursor declaration
> BEGIN
    DECLARE x INT;
    DECLARE my_cursor CURSOR FOR SELECT id FROM range(5);

    OPEN my_cursor;
    FETCH my_cursor INTO x;
    VALUES (x);
    CLOSE my_cursor;
  END;
0

-- Cursor with parameter markers
> BEGIN
    DECLARE x INT;
    DECLARE filtered_cursor CURSOR FOR
      SELECT id FROM range(10) WHERE id >= ? AND id <= ?;

    OPEN filtered_cursor USING 3, 7;
    FETCH filtered_cursor INTO x;
    VALUES (x);
    CLOSE filtered_cursor;
  END;
3

-- Cursor with variable reference
> BEGIN
    DECLARE threshold INT DEFAULT 5;
    DECLARE x INT;
    DECLARE var_cursor CURSOR FOR
      SELECT id FROM range(10) WHERE id > threshold;

    OPEN var_cursor;
    FETCH var_cursor INTO x;
    VALUES (x);
    CLOSE var_cursor;
  END;
6

-- Cursor with NOT FOUND handler for iteration
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

## Notes

- Cursors must be declared in the declaration section of a compound statement, before any executable statements.
- Cursor declarations must appear after variable and condition declarations, but before handler declarations.
- The cursor name is scoped to the compound statement where it is declared.
- Multiple cursors with the same name can exist in nested compound statements. Use label qualification (`label.cursor_name`) to access cursors from outer scopes.
- Cursors are implicitly closed when:
  - The compound statement that declares them exits
  - An `EXIT` handler is triggered
- Parameter binding occurs at `OPEN` time, not at `DECLARE` time.

## Related articles

- [Compound Statement](../control-flow/compound-stmt.html)
- [OPEN Statement](../control-flow/open-stmt.html)
- [FETCH Statement](../control-flow/fetch-stmt.html)
- [CLOSE Statement](../control-flow/close-stmt.html)
- [SQL Scripting](../sql-ref-scripting.html)
