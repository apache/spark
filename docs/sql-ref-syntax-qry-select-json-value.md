---
layout: global
title: JSON_VALUE
displayTitle: JSON_VALUE
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

The `JSON_VALUE` scalar function extracts a single scalar value located by a SQL/JSON path from a
JSON document and returns it cast to the `RETURNING` type (`STRING` by default). This is the
SQL-standard way (SQL:2016) to pull an individual value out of JSON, and is commonly used to
migrate queries from other systems such as Oracle, DB2, and MySQL. Unlike
[JSON_TABLE](sql-ref-syntax-qry-select-json-table.html), which produces rows in a `FROM` clause,
`JSON_VALUE` is an expression that can appear anywhere a scalar is allowed.

The function returns a scalar only. A path that matches an object or array is an *error* case (see
`ON ERROR`), not a value. To extract an object or array as a JSON fragment, use
[JSON_QUERY](sql-ref-syntax-qry-select-json-query.html); to produce rows from a JSON array, use
[JSON_TABLE](sql-ref-syntax-qry-select-json-table.html) (the built-in `get_json_object` function
also extracts fragments).

### Syntax

```sql
JSON_VALUE ( json_expr, path
             [ RETURNING data_type ]
             [ empty_behavior ON EMPTY ]
             [ error_behavior ON ERROR ] )

empty_behavior
    { NULL | ERROR | DEFAULT default_expr }

error_behavior
    { NULL | ERROR | DEFAULT default_expr }
```

### Parameters

* **json_expr**

    An expression that evaluates to a `STRING` containing the JSON document. A `NULL` input yields
    `NULL` directly (it triggers neither the `ON EMPTY` nor the `ON ERROR` behavior).

* **path**

    A SQL/JSON path literal that locates the value, for example `'$.a.b'` or `'$.items[0]'`. The
    path must be wildcard-free, since `JSON_VALUE` returns a single value; a path containing `[*]`
    is rejected at analysis time.

* **RETURNING data_type**

    The type the extracted value is cast to. It must be a scalar (atomic) type: a string, numeric,
    boolean, or datetime type. Non-atomic types (`STRUCT`, `ARRAY`, `MAP`) and `VARIANT` / `BINARY`
    are not supported. If `RETURNING` is omitted, the result type is `STRING`.

* **empty_behavior ON EMPTY**

    What to produce when `path` matches nothing:
    * `NULL` (the default) returns SQL `NULL`.
    * `ERROR` raises an error.
    * `DEFAULT default_expr` returns `default_expr`, cast to the `RETURNING` type.

* **error_behavior ON ERROR**

    What to produce when the extraction fails: the input is not well-formed JSON, the path matches a
    non-scalar (object or array) value, or casting the matched scalar to the `RETURNING` type fails.
    * `NULL` (the default) returns SQL `NULL`.
    * `ERROR` raises an error.
    * `DEFAULT default_expr` returns `default_expr`, cast to the `RETURNING` type.

    The cast of the matched scalar to the `RETURNING` type always follows ANSI semantics (a failed
    conversion routes to `ON ERROR`), independently of the session's `spark.sql.ansi.enabled`
    setting.

A path that matches an explicit JSON `null` is a present scalar value and returns SQL `NULL` (it is
neither the `ON EMPTY` nor the `ON ERROR` case).

### Examples

```sql
-- Extract a scalar as STRING (the default)
SELECT json_value('{"id":7,"name":"Ada"}', '$.name');
+-------------------------------------------+
|json_value({"id":7,"name":"Ada"}, $.name)  |
+-------------------------------------------+
|Ada                                        |
+-------------------------------------------+

-- Cast the extracted value with RETURNING
SELECT json_value('{"id":7}', '$.id' RETURNING INT) + 1;
+---------------------------------------------+
|(json_value({"id":7}, $.id) + 1)             |
+---------------------------------------------+
|8                                            |
+---------------------------------------------+

-- A missing path defaults to NULL; supply a fallback with DEFAULT ... ON EMPTY
-- (RETURNING, when present, comes before the ON EMPTY / ON ERROR clauses)
SELECT json_value('{"id":7}', '$.missing' RETURNING INT DEFAULT -1 ON EMPTY);
+---------------------------------------------------------------+
|json_value({"id":7}, $.missing RETURNING INT DEFAULT -1 ON EMPTY)|
+---------------------------------------------------------------+
|-1                                                             |
+---------------------------------------------------------------+

-- A non-scalar match or malformed input is an ON ERROR case
SELECT json_value('{"addr":{"city":"NYC"}}', '$.addr' DEFAULT 'n/a' ON ERROR);
+---------------------------------------------------------------+
|json_value({"addr":{"city":"NYC"}}, $.addr DEFAULT n/a ON ERROR)|
+---------------------------------------------------------------+
|n/a                                                            |
+---------------------------------------------------------------+

-- ERROR ON ERROR raises instead of returning a value
SELECT json_value('not json', '$.a' ERROR ON ERROR);
[JSON_VALUE_ON_ERROR.ERROR] ...
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
* [JSON_TABLE](sql-ref-syntax-qry-select-json-table.html)
* [Built-in Functions](sql-ref-functions-builtin.html)
