---
layout: global
title: SHOW COLLATIONS
displayTitle: SHOW COLLATIONS
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

Returns the list of collations supported by Spark. An optional pattern may be used to filter
the results. The `LIKE` clause is optional.

### Syntax

```sql
SHOW COLLATIONS [ LIKE regex_pattern ]
```

### Parameters

* **regex_pattern**

    Specifies the regular expression pattern that is used to filter the results of the statement.

    * Except for `*` and `|` character, the pattern works like a regular expression.
    * `*` alone matches 0 or more characters and `|` is used to separate multiple different regular expressions,
      any of which can match.
    * The leading and trailing blanks are trimmed in the input pattern before processing. The pattern match is case-insensitive.

### Output

The output has the following columns:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| NAME | STRING | No | The name of the collation. |
| LANGUAGE | STRING | Yes | The display language of the locale, or `null` for locale-independent collations. |
| COUNTRY | STRING | Yes | The display country of the locale, or `null` for locale-independent collations. |
| ACCENT_SENSITIVITY | STRING | No | Whether the collation is accent-sensitive (`ACCENT_SENSITIVE`) or accent-insensitive (`ACCENT_INSENSITIVE`). |
| CASE_SENSITIVITY | STRING | No | Whether the collation is case-sensitive (`CASE_SENSITIVE`) or case-insensitive (`CASE_INSENSITIVE`). |
| PAD_ATTRIBUTE | STRING | No | The pad attribute of the collation: `NO_PAD` or `RTRIM`. |
| ICU_VERSION | STRING | Yes | The ICU library version used for the collation, or `null` for non-ICU collations such as `UTF8_BINARY` and `UTF8_LCASE`. |

### Examples

```sql
-- List all supported collations (results truncated)
SHOW COLLATIONS;
+-----------------+--------+-------+------------------+----------------+-------------+-----------+
|             NAME|LANGUAGE|COUNTRY|ACCENT_SENSITIVITY|CASE_SENSITIVITY|PAD_ATTRIBUTE|ICU_VERSION|
+-----------------+--------+-------+------------------+----------------+-------------+-----------+
|      UTF8_BINARY|    null|   null|  ACCENT_SENSITIVE|  CASE_SENSITIVE|       NO_PAD|       null|
|       UTF8_LCASE|    null|   null|  ACCENT_SENSITIVE|CASE_INSENSITIVE|       NO_PAD|       null|
|          UNICODE|    null|   null|  ACCENT_SENSITIVE|  CASE_SENSITIVE|       NO_PAD|       78.2|
|       UNICODE_CI|    null|   null|  ACCENT_SENSITIVE|CASE_INSENSITIVE|       NO_PAD|       78.2|
|           en_USA|  English| United States|ACCENT_SENSITIVE|CASE_SENSITIVE|  NO_PAD|       78.2|
|        en_USA_CI|  English| United States|ACCENT_SENSITIVE|CASE_INSENSITIVE| NO_PAD|       78.2|
|              ...|     ...|    ...|               ...|             ...|         ...|        ...|
+-----------------+--------+-------+------------------+----------------+-------------+-----------+

-- List all collations matching `UTF8_BINARY*`
SHOW COLLATIONS LIKE 'UTF8_BINARY*';
+-----------------+--------+-------+------------------+----------------+-------------+-----------+
|             NAME|LANGUAGE|COUNTRY|ACCENT_SENSITIVITY|CASE_SENSITIVITY|PAD_ATTRIBUTE|ICU_VERSION|
+-----------------+--------+-------+------------------+----------------+-------------+-----------+
|      UTF8_BINARY|    null|   null|  ACCENT_SENSITIVE|  CASE_SENSITIVE|       NO_PAD|       null|
| UTF8_BINARY_RTRIM|    null|   null|  ACCENT_SENSITIVE|  CASE_SENSITIVE|        RTRIM|       null|
+-----------------+--------+-------+------------------+----------------+-------------+-----------+

-- List all collations matching `UNICODE*`
SHOW COLLATIONS LIKE 'UNICODE*';
+--------------+--------+-------+-------------------+----------------+-------------+-----------+
|          NAME|LANGUAGE|COUNTRY| ACCENT_SENSITIVITY|CASE_SENSITIVITY|PAD_ATTRIBUTE|ICU_VERSION|
+--------------+--------+-------+-------------------+----------------+-------------+-----------+
|       UNICODE|    null|   null|   ACCENT_SENSITIVE|  CASE_SENSITIVE|       NO_PAD|       78.2|
|    UNICODE_AI|    null|   null| ACCENT_INSENSITIVE|  CASE_SENSITIVE|       NO_PAD|       78.2|
| UNICODE_AI_RTRIM|    null|   null|ACCENT_INSENSITIVE|CASE_SENSITIVE|        RTRIM|       78.2|
|    UNICODE_CI|    null|   null|   ACCENT_SENSITIVE|CASE_INSENSITIVE|       NO_PAD|       78.2|
| UNICODE_CI_AI|    null|   null| ACCENT_INSENSITIVE|CASE_INSENSITIVE|       NO_PAD|       78.2|
|UNICODE_CI_AI_RTRIM|    null|   null|ACCENT_INSENSITIVE|CASE_INSENSITIVE|      RTRIM|       78.2|
| UNICODE_CI_RTRIM|    null|   null|  ACCENT_SENSITIVE|CASE_INSENSITIVE|       RTRIM|       78.2|
|  UNICODE_RTRIM|    null|   null|   ACCENT_SENSITIVE|  CASE_SENSITIVE|        RTRIM|       78.2|
+--------------+--------+-------+-------------------+----------------+-------------+-----------+

-- List all collations matching `UNICODE` or `UTF8_BINARY`
SHOW COLLATIONS LIKE 'UNICODE|UTF8_BINARY';
+-----------+--------+-------+------------------+----------------+-------------+-----------+
|       NAME|LANGUAGE|COUNTRY|ACCENT_SENSITIVITY|CASE_SENSITIVITY|PAD_ATTRIBUTE|ICU_VERSION|
+-----------+--------+-------+------------------+----------------+-------------+-----------+
|UTF8_BINARY|    null|   null|  ACCENT_SENSITIVE|  CASE_SENSITIVE|       NO_PAD|       null|
|    UNICODE|    null|   null|  ACCENT_SENSITIVE|  CASE_SENSITIVE|       NO_PAD|       78.2|
+-----------+--------+-------+------------------+----------------+-------------+-----------+
```

### Related Statements

* [STRING TYPE](sql-ref-datatypes.html)
