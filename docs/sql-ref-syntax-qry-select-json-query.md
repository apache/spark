---
layout: global
title: JSON_QUERY
displayTitle: JSON_QUERY
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

The `JSON_QUERY` function extracts the JSON value located by a SQL/JSON path from a JSON document
and returns it as JSON text (a `STRING`). This is the SQL-standard way (SQL:2016) to pull an object,
array, or scalar fragment out of JSON, and is commonly used to migrate queries from other systems
such as Oracle, SQL Server, and Trino. Unlike
[JSON_TABLE](sql-ref-syntax-qry-select-json-table.html), which produces rows in a `FROM` clause,
`JSON_QUERY` is an expression that can appear anywhere a value is allowed.

Where [JSON_VALUE](sql-ref-syntax-qry-select-json-value.html) returns a single scalar (and treats an
object or array match as an error), `JSON_QUERY` returns the matched value serialized as JSON text,
whether it is an object, an array, or a scalar.

This implementation supports simple, wildcard-free SQL/JSON paths only. The `PASSING` clause, path
predicates and filters, and explicit `lax` / `strict` path modes defined by SQL:2016 are not
supported.

### Syntax

```sql
JSON_QUERY ( json_expr, path
             [ RETURNING data_type ]
             [ wrapper_behavior ]
             [ quotes_behavior ]
             [ empty_behavior ON EMPTY ]
             [ error_behavior ON ERROR ] )

wrapper_behavior
    { WITHOUT [ ARRAY ] WRAPPER
    | WITH [ CONDITIONAL | UNCONDITIONAL ] [ ARRAY ] WRAPPER }

quotes_behavior
    { KEEP QUOTES | OMIT QUOTES }

empty_behavior
    { NULL | ERROR | EMPTY ARRAY | EMPTY OBJECT }

error_behavior
    { NULL | ERROR | EMPTY ARRAY | EMPTY OBJECT }
```

### Parameters

* **json_expr**

    An expression that evaluates to a `STRING` containing the JSON document. A `NULL` input yields
    `NULL` directly (it triggers neither the `ON EMPTY` nor the `ON ERROR` behavior).

* **path**

    A SQL/JSON path literal that locates the value, for example `'$.a.b'` or `'$.items[0]'`. The
    path must be wildcard-free; a path containing `[*]` is rejected at analysis time.

* **RETURNING data_type**

    The type of the result. It must be a string type; the result is JSON text. If `RETURNING` is
    omitted, the result type is `STRING`.

* **wrapper_behavior**

    Whether to wrap the result in a JSON array:
    * `WITHOUT ARRAY WRAPPER` (the default) returns the value unwrapped.
    * `WITH UNCONDITIONAL ARRAY WRAPPER` (or simply `WITH ARRAY WRAPPER`) always wraps the value in a
      one-element array.
    * `WITH CONDITIONAL ARRAY WRAPPER` wraps the value only when it is a scalar; an object or array
      is returned unwrapped.

* **quotes_behavior**

    Whether to keep the surrounding quotes of a scalar string result:
    * `KEEP QUOTES` (the default) leaves them, so a string is returned as a quoted JSON string.
    * `OMIT QUOTES` strips them, returning the raw string content. It is a no-op for objects,
      arrays, and non-string scalars, and cannot be combined with an array wrapper.

* **empty_behavior ON EMPTY**

    What to produce when `path` matches nothing:
    * `NULL` (the default) returns SQL `NULL`.
    * `ERROR` raises an error.
    * `EMPTY ARRAY` returns the JSON text `[]`.
    * `EMPTY OBJECT` returns the JSON text `{}`.

* **error_behavior ON ERROR**

    What to produce when the input is not well-formed JSON. The same four choices as `ON EMPTY`
    apply, defaulting to `NULL`.

A path that matches an explicit JSON `null` is a present scalar value and returns the JSON text
`null` (it is neither the `ON EMPTY` nor the `ON ERROR` case).

Returning a scalar under the default `WITHOUT ARRAY WRAPPER` is an intentional convenience: the
matched scalar is emitted as JSON text (for example, `JSON_QUERY('{"id":7}', '$.id')` returns `7`),
whereas strict SQL:2016 treats a scalar without a wrapper as an error. The wrapper clauses behave the
standard way: `WITH CONDITIONAL ARRAY WRAPPER` wraps a scalar in a one-element array (`7` becomes
`[7]`) while leaving a single object or array unwrapped, and `WITH UNCONDITIONAL ARRAY WRAPPER`
always wraps.

### Examples

```sql
-- Extract an object as JSON text
SELECT json_query('{"id":7,"addr":{"city":"NYC"}}', '$.addr');
+---------------------------------------------------+
|json_query({"id":7,"addr":{"city":"NYC"}}, $.addr) |
+---------------------------------------------------+
|{"city":"NYC"}                                     |
+---------------------------------------------------+

-- Extract an array
SELECT json_query('{"tags":["x","y"]}', '$.tags');
+-------------------------------------------+
|json_query({"tags":["x","y"]}, $.tags)     |
+-------------------------------------------+
|["x","y"]                                  |
+-------------------------------------------+

-- Wrap a scalar in an array with WITH ARRAY WRAPPER
-- (WITH ARRAY WRAPPER is a shorthand; the column name shows the canonical
--  WITH UNCONDITIONAL ARRAY WRAPPER form)
SELECT json_query('{"tags":["x","y"]}', '$.tags[0]' WITH ARRAY WRAPPER);
+----------------------------------------------------------------------------+
|json_query({"tags":["x","y"]}, $.tags[0] WITH UNCONDITIONAL ARRAY WRAPPER)  |
+----------------------------------------------------------------------------+
|["x"]                                                                       |
+----------------------------------------------------------------------------+

-- Strip the quotes from a scalar string with OMIT QUOTES
SELECT json_query('{"name":"Ada"}', '$.name' OMIT QUOTES);
+---------------------------------------------------+
|json_query({"name":"Ada"}, $.name OMIT QUOTES)     |
+---------------------------------------------------+
|Ada                                                |
+---------------------------------------------------+

-- A missing path defaults to NULL; supply a fallback with EMPTY ARRAY ON EMPTY
SELECT json_query('{"id":7}', '$.missing' EMPTY ARRAY ON EMPTY);
+---------------------------------------------------------+
|json_query({"id":7}, $.missing EMPTY ARRAY ON EMPTY)     |
+---------------------------------------------------------+
|[]                                                       |
+---------------------------------------------------------+

-- ERROR ON ERROR raises instead of returning a value
SELECT json_query('not json', '$.a' ERROR ON ERROR);
[JSON_QUERY_ON_ERROR.ERROR] ...
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
* [JSON_VALUE](sql-ref-syntax-qry-select-json-value.html)
* [JSON_TABLE](sql-ref-syntax-qry-select-json-table.html)
* [Built-in Functions](sql-ref-functions-builtin.html)
