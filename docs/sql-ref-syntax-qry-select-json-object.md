---
layout: global
title: JSON_OBJECT
displayTitle: JSON_OBJECT
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

The `JSON_OBJECT` constructor function builds a JSON object from key/value pairs and returns it as
JSON text. This is the SQL-standard way (SQL:2016) to assemble a JSON object inline, with common
compatibility syntax from other systems. `JSON_OBJECT` is an expression that can appear anywhere a
value is allowed.

Each value is serialized with the same JSON writer as the built-in `to_json` function, so numbers,
decimals, booleans, dates, timestamps, and nested structs/arrays/maps render the same way.
Null-field handling inside a struct value therefore follows
`spark.sql.jsonGenerator.ignoreNullFields`, exactly as `to_json` does; the `ON NULL` clause below
controls only the top-level object members.

This is an initial subset of the SQL:2016 `JSON_OBJECT` constructor. It supports the key/value
members, the `{ NULL | ABSENT } ON NULL` clause, and a string-type `RETURNING`. The following
SQL/JSON clauses are not yet supported:

* The value-level `FORMAT JSON` marker (which tags a string value as pre-formatted JSON to be
  spliced in raw). A nested `JSON_OBJECT` is still spliced in as raw JSON; see the **value**
  parameter below.
* The `{ WITH | WITHOUT } UNIQUE KEYS` clause. Duplicate keys are kept in source order (the
  `WITHOUT UNIQUE KEYS` behavior); there is no option to reject them.
* `RETURNING` to a non-string type, or with a binary/`FORMAT JSON` output clause. The result is
  always JSON text of a string type.

### Syntax

```sql
JSON_OBJECT ( [ { key VALUE value | KEY key VALUE value | key : value } [, ...] ]
              [ { NULL | ABSENT } ON NULL ]
              [ RETURNING data_type ] )

JSON_OBJECT ( [ key, value [, key, value] ... ]
              [ { NULL | ABSENT } ON NULL ]
              [ RETURNING data_type ] )
```

### Parameters

* **key**

    An expression producing a member name. It must be a non-null string; a `NULL` key raises an
    error at runtime. The key is validated before the `ON NULL` handling, so a `NULL` key raises the
    error even when the member's value is `NULL` and `ABSENT ON NULL` would otherwise omit it.

* **value**

    An expression producing the member value. Values may have different types and may be nested
    `JSON_OBJECT` constructors. `JSON_OBJECT()` with no members produces the empty object `{}`. A
    nested JSON constructor is spliced in as raw JSON (e.g. `JSON_OBJECT('a' VALUE JSON_OBJECT('b'
    VALUE 1))` produces `{"a":{"b":1}}`); an explicit value-level `FORMAT JSON` clause is not yet
    supported.

* **{ NULL | ABSENT } ON NULL**

    How to handle a member whose `value` is `NULL`:
    * `NULL ON NULL` (the default) keeps the member with a JSON `null` value.
    * `ABSENT ON NULL` omits the member entirely.

* **RETURNING data_type**

    The type of the result. It must be a string type; the result is JSON text. If `RETURNING` is
    omitted, the result type is `STRING`. `CHAR` / `VARCHAR` are normalized to `STRING` (the length
    is not enforced, because the fragment is serialized directly).

### Examples

```sql
-- Construct an object from key/value pairs
SELECT json_object('id' VALUE 7, 'name' VALUE 'Ada');
+-------------------------------------------+
|json_object('id' VALUE 7, 'name' VALUE Ada)|
+-------------------------------------------+
|{"id":7,"name":"Ada"}                      |
+-------------------------------------------+

-- KEY before the member name is also accepted
SELECT json_object(KEY 'id' VALUE 7, KEY 'name' VALUE 'Ada') AS obj;
+---------------------+
|obj                  |
+---------------------+
|{"id":7,"name":"Ada"}|
+---------------------+

-- The `:` separator is equivalent to VALUE; NULL ON NULL (the default) keeps null members
SELECT json_object('id' : 7, 'v' : NULL) AS obj;
+-----------------+
|obj              |
+-----------------+
|{"id":7,"v":null}|
+-----------------+

-- Comma-separated key/value pairs are also accepted
SELECT json_object('id', 7, 'name', 'Ada') AS obj;
+---------------------+
|obj                  |
+---------------------+
|{"id":7,"name":"Ada"}|
+---------------------+

-- ABSENT ON NULL omits members whose value is NULL
SELECT json_object('id' : 7, 'v' : NULL ABSENT ON NULL) AS obj;
+--------+
|obj     |
+--------+
|{"id":7}|
+--------+

-- A nested JSON_OBJECT composes and is spliced in raw
SELECT json_object('a' VALUE json_object('b' VALUE 1));
+-----------------------------------------------+
|json_object('a' VALUE json_object('b' VALUE 1))|
+-----------------------------------------------+
|{"a":{"b":1}}                                  |
+-----------------------------------------------+
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
* [JSON_VALUE](sql-ref-syntax-qry-select-json-value.html)
* [JSON_TABLE](sql-ref-syntax-qry-select-json-table.html)
* [Built-in Functions](sql-ref-functions-builtin.html)
