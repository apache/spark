---
layout: global
title: UNNEST Clause
displayTitle: UNNEST Clause
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

The `UNNEST` clause expands one or more arrays into a table that can be referenced in the
`FROM` clause, producing one row per array element. It is the ANSI SQL collection derived table
and is a standard alternative to `explode` / `LATERAL VIEW`.

When several arrays are supplied, they are expanded in parallel: the number of output rows equals
the length of the longest array, and shorter arrays are padded with `NULL`s. A `NULL` array is
treated as an empty array and contributes no elements.

To reference a column of another `FROM` item (a correlated array), use `UNNEST` on the right-hand
side of a `LATERAL` join. A `LEFT JOIN LATERAL ... ON true` preserves outer rows whose array is
empty or `NULL`.

`UNNEST` is a non-reserved keyword, so it can still be used as a regular table or column name.
However, when it appears unquoted at the start of a `FROM` relation followed by `(`, it is parsed
as the `UNNEST` clause described here rather than as a call to a table-valued function named
`unnest`. To invoke such a function instead, quote the name: `` `unnest`(...) ``.

### Syntax

```sql
UNNEST ( expression [ , ... ] ) [ WITH ORDINALITY ] [ table_alias ]
```

### Parameters

* **expression**

    One or more array-typed expressions to expand. Each array contributes one output column,
    holding its element as-is (an array of structs is not expanded into one column per field).

* **WITH ORDINALITY**

    Appends a trailing 1-based `BIGINT` column giving the position of each element.

* **table_alias**

    Specifies a temporary name with an optional column name list.

    **Syntax:** `[ AS ] table_name [ ( column_name [ , ... ] ) ]`

### Examples

```sql
-- single array
SELECT * FROM UNNEST(array(10, 20, 30));
+---+
|col|
+---+
| 10|
| 20|
| 30|
+---+

-- WITH ORDINALITY and a table alias
SELECT * FROM UNNEST(array('a', 'b')) WITH ORDINALITY AS t(value, pos);
+-----+---+
|value|pos|
+-----+---+
|    a|  1|
|    b|  2|
+-----+---+

-- multiple arrays are expanded in parallel and padded with NULLs
SELECT * FROM UNNEST(array(1, 2), array(10, 20, 30)) AS t(a, b);
+----+---+
|   a|  b|
+----+---+
|   1| 10|
|   2| 20|
|NULL| 30|
+----+---+

-- correlated UNNEST over a table column, via LATERAL
SELECT id, elem
FROM VALUES (1, array(10, 20)), (2, array(30)) AS data(id, arr),
     LATERAL UNNEST(arr) AS t(elem);
+---+----+
| id|elem|
+---+----+
|  1|  10|
|  1|  20|
|  2|  30|
+---+----+
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
* [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
* [Table-valued Function](sql-ref-syntax-qry-select-tvf.html)
