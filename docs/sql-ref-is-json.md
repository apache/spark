---
layout: global
title: IS JSON predicate
displayTitle: IS JSON predicate
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

The `IS JSON` predicate tests whether a string is a well-formed JSON text, optionally of a specific
top-level type. It implements the ANSI SQL:2016 feature T801 and is available since Spark 4.4.0.

### Syntax

```sql
expr IS [ NOT ] JSON [ VALUE | OBJECT | ARRAY | SCALAR ]
```

### Parameters

* **expr**

    An expression of type `STRING` (any collation).

* **NOT**

    Negates the result. `expr IS NOT JSON ...` is equivalent to `NOT (expr IS JSON ...)`.

* **VALUE**

    Accepts any well-formed JSON value: an object, an array, or a scalar. This is the default, so
    `expr IS JSON` and `expr IS JSON VALUE` are equivalent.

* **OBJECT**

    Accepts only a JSON object, such as `{"a": 1}`.

* **ARRAY**

    Accepts only a JSON array, such as `[1, 2]`.

* **SCALAR**

    Accepts only a JSON scalar: a string, a number, `true`, `false`, or `null`.

### Returns

A `BOOLEAN`. The result is `NULL` if `expr` is `NULL`; otherwise it is `true` or `false`. Note that
the JSON literal `null` is a well-formed JSON scalar, so `'null' IS JSON` returns `true`, while
`CAST(NULL AS STRING) IS JSON` returns `NULL`.

### Well-formedness

The input must be exactly one JSON value as defined by RFC 8259, optionally surrounded by
whitespace. Unlike the lenient parsing used by functions such as `from_json` and `json_tuple`,
`IS JSON` does not accept any extensions. In particular, the predicate returns `false` for:

* empty input or input that contains only whitespace;
* more than one value, or trailing content after the value, such as `1 2` or `{} []`;
* single-quoted strings or field names, such as `{'a': 1}`;
* unquoted field names, comments, `NaN`, `Infinity`, or numbers with leading zeros;
* unescaped control characters (for example, a raw newline) inside a string;
* input that is not valid UTF-8;
* string values or field names that contain an unpaired UTF-16 surrogate escape, such as
  `"\uD835"`.

The `WITH UNIQUE KEYS` and `WITHOUT UNIQUE KEYS` qualifiers of the SQL standard are not supported.
Objects with duplicate field names are accepted.

### Examples

```sql
SELECT '{"a": 1}' IS JSON, '{"a": 1}' IS JSON OBJECT, '{"a": 1}' IS JSON ARRAY;
+------------------+-------------------------+------------------------+
|({"a": 1} IS JSON)|({"a": 1} IS JSON OBJECT)|({"a": 1} IS JSON ARRAY)|
+------------------+-------------------------+------------------------+
|true              |true                     |false                   |
+------------------+-------------------------+------------------------+

SELECT '[1, 2]' IS JSON ARRAY, '42' IS JSON SCALAR, 'null' IS JSON VALUE;
+----------------------+-------------------+--------------------+
|([1, 2] IS JSON ARRAY)|(42 IS JSON SCALAR)|(null IS JSON VALUE)|
+----------------------+-------------------+--------------------+
|true                  |true               |true                |
+----------------------+-------------------+--------------------+

-- Malformed or non-standard JSON is rejected.
SELECT '{''a'': 1}' IS JSON, '1 2' IS JSON, 'abc' IS NOT JSON;
+------------------+-------------+-------------------+
|({'a': 1} IS JSON)|(1 2 IS JSON)|(NOT (abc IS JSON))|
+------------------+-------------+-------------------+
|false             |false        |true               |
+------------------+-------------+-------------------+

-- A NULL input yields NULL.
SELECT CAST(NULL AS STRING) IS JSON;
+------------------------------+
|(CAST(NULL AS STRING) IS JSON)|
+------------------------------+
|NULL                          |
+------------------------------+
```

### Related

* [Operators](sql-ref-operators.html)
* [Null Semantics](sql-ref-null-semantics.html)
* [JSON Functions](sql-ref-functions-builtin.html#json-functions)
