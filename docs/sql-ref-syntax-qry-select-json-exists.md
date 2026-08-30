---
layout: global
title: JSON_EXISTS
displayTitle: JSON_EXISTS
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

The `JSON_EXISTS` predicate tests whether a SQL/JSON path matches at least one item in a JSON
document, returning a `BOOLEAN`. This is the SQL-standard (SQL:2016) way to test for the presence
of a JSON value, and is commonly used to migrate queries from other systems such as Oracle, DB2,
and PostgreSQL.

Unlike `get_json_object(json_expr, path) IS NOT NULL`, `JSON_EXISTS` distinguishes a path that is
_present but whose value is JSON `null`_ (which is `true`) from a path that is _absent_ (which is
`false`).

### Syntax

```sql
JSON_EXISTS ( json_expr, path [ { TRUE | FALSE | UNKNOWN | ERROR } ON ERROR ] )
```

### Parameters

* **json_expr**

    An expression that evaluates to a `STRING` containing the JSON document. A `NULL` input yields
    `NULL` (SQL Unknown), regardless of the `ON ERROR` clause.

* **path**

    A constant SQL/JSON path literal (for example `'$.a.b'`, `'$.tags[0]'`, or `'$.a[*].b'`). Paths
    are evaluated in **lax** mode, matching Oracle and PostgreSQL: array wildcards (`[*]`) and member
    wildcards (`.*` / `['*']`) are supported, and arrays are auto-wrapped/unwrapped (a member, index,
    or wildcard step applied to an array is applied to each element, and a non-array value is treated
    as a single-element array). A structural mismatch is a non-match, not an error. A syntactically
    invalid path is rejected during analysis.

* **{ TRUE | FALSE | UNKNOWN | ERROR } ON ERROR**

    Controls the result when `json_expr` is not a single well-formed JSON value (malformed input,
    or a valid value followed by trailing content). `TRUE`, `FALSE`, and `UNKNOWN` produce that
    value (`UNKNOWN` is a `BOOLEAN` `NULL`); `ERROR` raises an error. The default is
    `FALSE ON ERROR`.

### Result

* The path matches at least one item (including a match whose value is JSON `null`) &rarr; `true`.
* The path matches nothing &rarr; `false`.
* `json_expr` is SQL `NULL` &rarr; `NULL`.
* `json_expr` is not a single well-formed JSON value &rarr; the `ON ERROR` behavior.

A structural mismatch is treated as "no match" (`false`), not an error -- for example reading an
absent key, reading a key from a scalar, an out-of-range array index, or `[*]` over an empty array.

### Examples

```sql
SELECT json_exists('{"a":{"b":1}}', '$.a.b') AS matched;
+-------+
|matched|
+-------+
|   true|
+-------+

-- Present but JSON null -> true; absent -> false
SELECT json_exists('{"a":null}', '$.a') AS present_null,
       json_exists('{"a":1}', '$.b')    AS absent;
+------------+------+
|present_null|absent|
+------------+------+
|        true| false|
+------------+------+

-- NULL input -> NULL (Unknown), regardless of the ON ERROR clause
SELECT json_exists(CAST(NULL AS STRING), '$.a' TRUE ON ERROR) AS r;
+----+
|   r|
+----+
|NULL|
+----+

-- Malformed input follows the ON ERROR clause (default FALSE)
SELECT json_exists('not json', '$.a')                 AS default_false,
       json_exists('not json', '$.a' TRUE ON ERROR)    AS true_on_error,
       json_exists('not json', '$.a' UNKNOWN ON ERROR) AS unknown_on_error;
+-------------+-------------+----------------+
|default_false|true_on_error|unknown_on_error|
+-------------+-------------+----------------+
|        false|         true|            NULL|
+-------------+-------------+----------------+

-- Lax wildcards: [*] is true iff the array has elements; auto-unwrap applies a step to each element
SELECT json_exists('{"a":[1,2]}', '$.a[*]')             AS has_elems,
       json_exists('{"a":[]}', '$.a[*]')                AS empty_array,
       json_exists('{"a":[{"b":1},{"c":2}]}', '$.a[*].b') AS any_elem_has_b;
+---------+-----------+--------------+
|has_elems|empty_array|any_elem_has_b|
+---------+-----------+--------------+
|     true|      false|          true|
+---------+-----------+--------------+

-- Use as a predicate in WHERE
SELECT id FROM docs WHERE json_exists(doc, '$.address.city');
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
* [WHERE Clause](sql-ref-syntax-qry-select-where.html)
* [JSON_TABLE](sql-ref-syntax-qry-select-json-table.html)
