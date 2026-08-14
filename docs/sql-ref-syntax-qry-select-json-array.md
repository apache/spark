---
layout: global
title: JSON_ARRAY
displayTitle: JSON_ARRAY
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

The `JSON_ARRAY` constructor function builds a JSON array from a list of argument values and
returns it as JSON text. This is the SQL-standard way (SQL:2016) to assemble a JSON array inline,
and is commonly used to migrate queries from other systems such as Oracle, DB2, and MySQL.
`JSON_ARRAY` is an expression that can appear anywhere a value is allowed.

Each argument is serialized with the same JSON writer as the built-in `to_json` function, so
numbers, decimals, booleans, dates, timestamps, and nested structs/arrays/maps render the same way.
Null-field handling inside a struct argument therefore follows
`spark.sql.jsonGenerator.ignoreNullFields`, exactly as `to_json` does; the `ON NULL` clause below
controls only the top-level array elements.

### Syntax

```sql
JSON_ARRAY ( [ value [ FORMAT JSON ] [, ...] ]
             [ { NULL | ABSENT } ON NULL ]
             [ RETURNING data_type ] )
```

### Parameters

* **value**

    An expression producing an element of the array. Arguments may have different types and may be
    nested `JSON_ARRAY` constructors. `JSON_ARRAY()` with no arguments produces the empty array
    `[]`.

* **FORMAT JSON**

    Marks a string `value` as already-JSON text, so it is spliced into the array verbatim instead
    of being quoted as a JSON string. For example, `JSON_ARRAY('[1,2]')` produces `["[1,2]"]`, while
    `JSON_ARRAY('[1,2]' FORMAT JSON)` produces `[[1,2]]`. A nested `JSON_ARRAY` constructor carries
    `FORMAT JSON` implicitly, so `JSON_ARRAY(JSON_ARRAY(1))` produces `[[1]]`. `FORMAT JSON` requires
    a string argument (an untyped `NULL` literal is also accepted and follows the `ON NULL`
    behavior, exactly as a non-`FORMAT JSON` `NULL` would). At runtime, a non-null `FORMAT JSON`
    value must contain exactly one well-formed JSON value; malformed text or multiple top-level
    values raise an error. This decision is fixed from the query text and does not depend on query
    optimization: a `JSON_ARRAY` result that reaches an argument through a column reference is a
    plain `STRING` and is quoted, whether or not the optimizer inlines it.

* **{ NULL | ABSENT } ON NULL**

    How to handle a `NULL` element:
    * `ABSENT ON NULL` (the default) omits `NULL` elements from the array.
    * `NULL ON NULL` keeps them as JSON `null` values.

* **RETURNING data_type**

    The type of the result. It must be a string type; the result is JSON text. If `RETURNING` is
    omitted, the result type is `STRING`. `CHAR` / `VARCHAR` are normalized to `STRING` (the length
    is not enforced, because the fragment is serialized directly).

### Examples

```sql
-- Construct an array from a mixed value list
SELECT json_array(1, 'x', true);
+---------------------------+
|json_array(1, x, true)     |
+---------------------------+
|[1,"x",true]               |
+---------------------------+

-- ABSENT ON NULL (the default) drops NULL elements
SELECT json_array(1, NULL, 3);
+------------------------+
|json_array(1, NULL, 3)  |
+------------------------+
|[1,3]                   |
+------------------------+

-- NULL ON NULL keeps them as JSON null
SELECT json_array(1, NULL, 3 NULL ON NULL);
+--------------------------------------+
|json_array(1, NULL, 3 NULL ON NULL)   |
+--------------------------------------+
|[1,null,3]                            |
+--------------------------------------+

-- A nested JSON_ARRAY is spliced in raw (implicit FORMAT JSON)
SELECT json_array(json_array(1, 2), 3);
+---------------------------------+
|json_array(json_array(1, 2), 3)  |
+---------------------------------+
|[[1,2],3]                        |
+---------------------------------+

-- FORMAT JSON splices an already-JSON string verbatim
SELECT json_array('[1,2]' FORMAT JSON);
+----------------------------------+
|json_array([1,2] FORMAT JSON)     |
+----------------------------------+
|[[1,2]]                           |
+----------------------------------+
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
* [JSON_VALUE](sql-ref-syntax-qry-select-json-value.html)
* [JSON_TABLE](sql-ref-syntax-qry-select-json-table.html)
* [Built-in Functions](sql-ref-functions-builtin.html)
