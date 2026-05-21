---
layout: global
title: current_path function
displayTitle: current_path function
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

Returns the effective SQL Path for the current session as a comma-separated string of
qualified namespace names. See [`SET PATH`](sql-ref-syntax-aux-conf-mgmt-set-path.html) for a
description of what the path is, how it is gated, and how to change it, and
[Name Resolution](sql-ref-name-resolution.html) for how the path drives unqualified name
resolution.

### Syntax

```sql
current_path()

CURRENT_PATH
```

Like `current_user`, `current_schema`, and `current_catalog`, `current_path` accepts an empty
argument list or no parentheses at all.

### Arguments

This function takes no arguments.

### Returns

A non-nullable `STRING`. Each path entry is written as a dotted name with backticks added only
where required by Spark's identifier rules. Entries are separated by a single comma.

When the path contains the virtual `CURRENT_SCHEMA` marker, the marker is materialized as the
catalog-qualified current schema (`current_catalog.current_schema`) each time
`current_path()` is evaluated, so subsequent `USE SCHEMA` statements are reflected without
re-issuing `SET PATH`.

`current_path()` is a regular built-in function. It remains available, and returns the default
path, even when `spark.sql.path.enabled` is `false`.

### Examples

```sql
> SET spark.sql.path.enabled = true;

> SELECT current_path();
 system.builtin,system.session,spark_catalog.default

-- ANSI no-parens form returns the same value.
> SELECT CURRENT_PATH;
 system.builtin,system.session,spark_catalog.default

-- The output reflects the latest SET PATH.
> SET PATH = spark_catalog.default, system.builtin;
> SELECT current_path();
 spark_catalog.default,system.builtin

-- CURRENT_SCHEMA on the path is re-evaluated on every call.
> SET PATH = CURRENT_SCHEMA, system.builtin;
> USE spark_catalog.finance;
> SELECT current_path();
 spark_catalog.finance,system.builtin
> USE spark_catalog.default;
> SELECT current_path();
 spark_catalog.default,system.builtin

-- Inside a persisted view or SQL function body, current_path() returns the invoker's path,
-- not the frozen path captured at creation time.
> SET PATH = spark_catalog.default, system.builtin;
> CREATE VIEW v_path AS SELECT current_path() AS p;
> SET PATH = spark_catalog.other, system.builtin;
> SELECT * FROM v_path;
 spark_catalog.other,system.builtin

-- current_path() still returns the default path when SET PATH is disabled.
> SET spark.sql.path.enabled = false;
> SELECT current_path();
 system.builtin,system.session,spark_catalog.default
```

### Related Statements

* [Name Resolution](sql-ref-name-resolution.html)
* [SET PATH](sql-ref-syntax-aux-conf-mgmt-set-path.html)
* [Built-in Functions](sql-ref-functions-builtin.html)
