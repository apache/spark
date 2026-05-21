---
layout: global
title: Identifiers
displayTitle: Identifiers
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

An identifier is a string used to identify a database object such as a table, view, schema, column, etc. Spark SQL has regular identifiers and delimited identifiers, which are enclosed within backticks. Both regular identifiers and delimited identifiers are case-insensitive.

### Syntax

#### Regular Identifier

```sql
{ letter | digit | '_' } [ , ... ]
```
**Note:** If `spark.sql.ansi.enforceReservedKeywords` is set to true, ANSI SQL reserved keywords cannot be used as identifiers. For more details, please refer to [ANSI Compliance](sql-ref-ansi-compliance.html).

#### Delimited Identifier

```sql
`c [ ... ]`
```

### Parameters

* **letter**

    Any letter from A-Z or a-z.

* **digit**

    Any numeral from 0 to 9.

* **c**

    Any character from the character set. Use <code>`</code> to escape special characters (e.g., <code>`</code>).

### Reserved system names

`system`, `session`, and `builtin` have special meaning and should not be used as user-defined
catalog or schema names.

| Name | Position | Notes |
| :--- | :------- | :---- |
| `system` | catalog | Synthetic catalog hosting `system.builtin` and `system.session`. Not loadable as a v2 catalog plugin; `spark.sql.catalog.system = ...` is unsupported, and the current catalog cannot be `system`. |
| `builtin` | schema | A persistent schema literally named `builtin` is allowed but discouraged: `builtin.x` follows the mini-path below. To reach a persistent `builtin` schema, use `current_catalog.builtin.x`. |
| `session` | schema | A persistent schema literally named `session` is allowed but discouraged: `session.x` follows the mini-path below. To reach a persistent `session` schema, use `current_catalog.session.x`. |

For 2-part references starting with `builtin` or `session`, Spark chooses the implicit catalog
using the mini-path below and returns the first match:

| `spark.sql.legacy.persistentCatalogFirst` | Mini-path tried in order |
| :-------------------------------------- | :----------------------- |
| `false` (default) | `system.session.x` / `system.builtin.x`, then `current_catalog.session.x` / `current_catalog.builtin.x` |
| `true` (legacy)   | `current_catalog.session.x` / `current_catalog.builtin.x`, then `system.session.x` / `system.builtin.x` |

3-part names skip the mini-path: `system.session.x` and `system.builtin.x` always target the
system namespace, and `current_catalog.session.x` / `current_catalog.builtin.x` always target the
persistent schema.

The `system.builtin` and `system.session` namespaces are described in
[SET PATH](sql-ref-syntax-aux-conf-mgmt-set-path.html). Temporary objects in `system.session` are
documented under [CREATE VIEW](sql-ref-syntax-ddl-create-view.html) and
[CREATE FUNCTION (SQL)](sql-ref-syntax-ddl-create-sql-function.html).

### Examples

```sql
-- This CREATE TABLE fails with ParseException because of the illegal identifier name a.b
CREATE TABLE test (a.b int);
Error in query:
[PARSE_SYNTAX_ERROR] Syntax error at or near '.': extra input '.'(line 1, pos 20)

== SQL ==
CREATE TABLE test (a.b int)
--------------------^^^

-- This CREATE TABLE works
CREATE TABLE test (`a.b` int);

-- This CREATE TABLE fails with ParseException because special character ` is not escaped
CREATE TABLE test1 (`a`b` int);
Error in query:
[PARSE_SYNTAX_ERROR] Syntax error at or near '`'(line 1, pos 24)

== SQL ==
CREATE TABLE test1 (`a`b` int)
------------------------^^^

-- This CREATE TABLE works
CREATE TABLE test (`a``b` int);
```
