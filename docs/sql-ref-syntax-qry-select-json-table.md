---
layout: global
title: JSON_TABLE
displayTitle: JSON_TABLE
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

The `JSON_TABLE` table-valued function shreds a JSON document into a relational table. A
_row path_ selects a sequence of JSON items, and a `COLUMNS` clause projects a value out of each
item into a typed column. This is the SQL-standard way (SQL:2016) to turn JSON into rows and
columns, and is commonly used to migrate queries from other systems such as Oracle, DB2, and
MySQL.

Only the flat (non-nested) form is currently supported. `NESTED PATH` columns are not yet
supported.

### Syntax

```sql
JSON_TABLE ( json_expr, row_path COLUMNS ( column_definition [ , ... ] ) [ error_clause ] ) [ table_alias ]

column_definition
    { column_name FOR ORDINALITY
    | column_name data_type [ PATH json_path ]
    | column_name data_type EXISTS [ PATH json_path ] }

error_clause
    { NULL | ERROR } ON ERROR
```

### Parameters

* **json_expr**

    An expression that evaluates to a `STRING` containing the JSON document.

* **row_path**

    A SQL/JSON path literal that selects the row source. A path ending in `[*]` (for example
    `'$.items[*]'`) selects each element of the matched array as a separate row. Any other path
    (for example `'$'`) selects a single value as one row. If the path matches nothing, no rows
    are produced.

* **column_name FOR ORDINALITY**

    Declares a `BIGINT` column that is a 1-based sequential counter of the generated rows.

* **column_name data_type [ PATH json_path ]**

    A value column. The value at `json_path` (relative to a row item) is extracted and cast to
    `data_type`. If `PATH` is omitted, the path defaults to the column name read as a single
    object key: a simple identifier maps like `$.name`, while a name containing special characters
    such as a dot is treated as one literal key (for example a column named `a.b` reads the key
    `"a.b"`, equivalent to `$['a.b']`, not the nested path `a` -> `b`). If the path matches nothing,
    the column is `NULL`.

* **column_name data_type EXISTS [ PATH json_path ]**

    An existence column. Evaluates to a truthy value when `json_path` matches and a falsy value
    otherwise, cast to `data_type` (for example `BOOLEAN`).

* **{ NULL | ERROR } ON ERROR**

    Controls behavior when `json_expr` is `NULL` or not well-formed JSON. `NULL ON ERROR` (the
    default) produces no rows. `ERROR ON ERROR` raises an error.

* **table_alias**

    An optional alias for the output, optionally followed by a column alias list.

### Examples

```sql
-- Expand a JSON array into rows with typed columns and an ordinality counter
SELECT t.* FROM JSON_TABLE(
  '{"items":[{"id":1,"n":"a"},{"id":2,"n":"b"}]}',
  '$.items[*]'
  COLUMNS (
    seq  FOR ORDINALITY,
    id   INT    PATH '$.id',
    name STRING PATH '$.n'
  )
) AS t;
+---+---+----+
|seq| id|name|
+---+---+----+
|  1|  1|   a|
|  2|  2|   b|
+---+---+----+

-- Implicit column path derived from the column name, and an EXISTS column
SELECT * FROM JSON_TABLE(
  '{"rows":[{"id":10,"opt":1},{"id":20}]}',
  '$.rows[*]'
  COLUMNS (id INT, hasOpt BOOLEAN EXISTS PATH '$.opt')
) AS t;
+---+------+
| id|hasOpt|
+---+------+
| 10|  true|
| 20| false|
+---+------+

-- Join JSON_TABLE output against a base table using LATERAL
SELECT d.id, t.k
FROM docs d,
LATERAL JSON_TABLE(d.doc, '$.tags[*]' COLUMNS (k STRING PATH '$.k')) AS t;
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
* [Table-valued Function](sql-ref-syntax-qry-select-tvf.html)
* [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
