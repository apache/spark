---
layout: global
title: SET PATH
displayTitle: SET PATH
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

`SET PATH` changes the **SQL Path** of the current session. The SQL Path is the ordered list of
namespaces Spark walks when resolving unqualified references to functions, tables, views, and
session variables. The first match along the path wins. See
[Name Resolution](sql-ref-name-resolution.html#sql-path) for the conceptual overview.

The path is observable through the [`current_path()`](sql-ref-function-current-path.html) function.

`SET PATH` is gated by `spark.sql.path.enabled`. When that configuration is `false` (the default),
`SET PATH` raises `UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED`. Unqualified resolution still uses
a fixed default path, and `current_path()` still returns it.

### Syntax

```sql
SET PATH = path_element [, path_element ...]

path_element
  : DEFAULT_PATH
  | SYSTEM_PATH
  | PATH
  | CURRENT_SCHEMA
  | CURRENT_DATABASE
  | schema_name

schema_name
  : { catalog_name . namespace_name [ . namespace_name ...] }
```

### Parameters

* **`DEFAULT_PATH`**

  Expands to the value of the `spark.sql.defaultPath` configuration, or, when that configuration
  is empty, to the spark-built-in default: `system.builtin`, `system.session`, and the current
  schema, interleaved per `spark.sql.functionResolution.sessionOrder`
  (`first` / `second` (default) / `last`).

* **`SYSTEM_PATH`**

  Expands to the two system namespaces `system.builtin` and `system.session`, in the order
  configured by `spark.sql.functionResolution.sessionOrder`.

* **`PATH`**

  Expands to the **current** value of the SQL Path. Useful for appending entries without
  re-typing them, for example `SET PATH = PATH, spark_catalog.analytics`.
  `PATH` is not allowed in the value of `spark.sql.defaultPath` (it would create a cycle).

* **`CURRENT_SCHEMA`** / **`CURRENT_DATABASE`**

  A virtual marker that resolves to the catalog-qualified current schema
  (`current_catalog.current_schema`) every time the path is consulted. This means subsequent
  `USE SCHEMA` statements are picked up without re-issuing `SET PATH`.
  `CURRENT_DATABASE` is a synonym for `CURRENT_SCHEMA`.

* **`schema_name`**

  An explicit catalog-qualified schema reference. At least two parts are required
  (`catalog.namespace`). The catalog and schema do not need to exist at the time of `SET PATH`;
  non-existent entries are silently skipped during name resolution.

  Identifier quoting follows the usual rules; backtick-quoted parts that contain a dot are
  preserved, for example `spark_catalog.` + `` `sch.b` ``.

### Description

* Duplicate entries are detected after expansion and raise `DUPLICATE_SQL_PATH_ENTRY`.
  Comparisons honor the session's case sensitivity setting.
* `CURRENT_SCHEMA` and `CURRENT_DATABASE` are aliases and are flagged as a duplicate if both are
  listed.
* Identifier case is preserved in storage and in `current_path()` output.
* Setting the path takes effect immediately. The change is scoped to the current session and is
  reset by `RESET` or by closing the session. Cloned sessions inherit the parent's path
  at clone time, but later changes in a child session do not propagate back.

#### How `DEFAULT_PATH` is derived

`DEFAULT_PATH` is what `SET PATH = DEFAULT_PATH` expands to, and it is also the effective path of
a session that has never issued `SET PATH`. It has two layers:

1. If `spark.sql.defaultPath` is set to a non-empty value, that value is parsed using the same
   grammar as `SET PATH` (with one restriction: the `PATH` keyword is not allowed inside the conf
   value, since it would be self-referential). The parsed value is `DEFAULT_PATH`.

   The conf value is validated for syntax at the time it is set; an invalid value is rejected
   with `Cannot modify the value of the SQL config 'spark.sql.defaultPath'`. Static duplicates
   inside the conf are tolerated (unlike interactive `SET PATH`, which rejects them) so a later
   `USE SCHEMA` cannot turn a previously valid default into a runtime error.

   A `DEFAULT_PATH` token *inside* the conf value resolves to the spark-built-in default below
   (cycle break) rather than recursing.

2. If `spark.sql.defaultPath` is empty (the factory default), `DEFAULT_PATH` is the
   **spark-built-in default**: `system.builtin`, `system.session`, and the current schema
   (`current_catalog.current_schema`), interleaved per
   `spark.sql.functionResolution.sessionOrder`:

   | `sessionOrder` | Order produced |
   | :------------- | :------------- |
   | `first` | `system.session`, `system.builtin`, `current_schema` |
   | `second` (default) | `system.builtin`, `system.session`, `current_schema` |
   | `last` | `system.builtin`, `current_schema`, `system.session` |

To change `DEFAULT_PATH`, set the conf via any of the usual mechanisms:

* In a session: `SET spark.sql.defaultPath = system.session, system.builtin, current_schema;`
* In static configuration: pass `--conf spark.sql.defaultPath=...` to `spark-submit`, set it in
  `SparkConf`, or add it to `spark-defaults.conf`.
* To return to the spark-built-in default, clear the conf with
  `RESET spark.sql.defaultPath` (or set it to an empty string).

`spark.sql.functionResolution.sessionOrder` and `spark.sql.legacy.persistentCatalogFirst` are
internal configurations intended for advanced use; ordinary workloads should leave them at their
defaults. See [Reserved names and collisions](sql-ref-name-resolution.html#reserved-names-and-collisions).

#### Error conditions

| Condition | Cause |
| :-------- | :---- |
| `UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED` | `SET PATH` was issued while `spark.sql.path.enabled` is `false`. |
| `INVALID_SQL_PATH_SCHEMA_REFERENCE` | A `schema_name` was given with fewer than two parts. |
| `DUPLICATE_SQL_PATH_ENTRY` | Two entries collapsed to the same concrete namespace after expansion. |

#### Reserved names

`system` is reserved as a catalog name and `session` / `builtin` are discouraged as schema names
because they collide with the 2-part shortcuts for the system namespaces. See
[Reserved names and collisions](sql-ref-name-resolution.html#reserved-names-and-collisions) for
the details and for the internal configurations that tune the behavior when collisions exist.

### Examples

```sql
-- Enable the feature first; the default is false.
> SET spark.sql.path.enabled = true;

-- Observe the default path.
> SELECT current_path();
 system.builtin,system.session,spark_catalog.default

-- Replace the path with explicit entries.
> SET PATH = spark_catalog.default, system.builtin;
> SELECT current_path();
 spark_catalog.default,system.builtin

-- Identifier case is preserved.
> SET PATH = Spark_Catalog.Default, System.Builtin;
> SELECT current_path();
 Spark_Catalog.Default,System.Builtin

-- Backtick-quoted parts that contain a dot round-trip with quoting.
> SET PATH = spark_catalog.`sch.b`, system.builtin;
> SELECT current_path();
 spark_catalog.`sch.b`,system.builtin

-- DEFAULT_PATH and SYSTEM_PATH shortcuts.
> SET PATH = DEFAULT_PATH;
> SET PATH = SYSTEM_PATH;
> SELECT current_path();
 system.builtin,system.session

-- Append an entry by referring to the current path.
> SET PATH = spark_catalog.default, system.builtin;
> SET PATH = PATH, spark_catalog.analytics;
> SELECT current_path();
 spark_catalog.default,system.builtin,spark_catalog.analytics

-- CURRENT_SCHEMA is a live marker; USE SCHEMA updates the effective path.
> SET PATH = CURRENT_SCHEMA, system.builtin;
> USE spark_catalog.finance;
> SELECT current_path();
 spark_catalog.finance,system.builtin
> USE spark_catalog.default;
> SELECT current_path();
 spark_catalog.default,system.builtin

-- DEFAULT_PATH can be customized via the conf.
> SET spark.sql.defaultPath = system.session, system.builtin, current_schema;
> SET PATH = DEFAULT_PATH;
> SELECT current_path();
 system.session,system.builtin,spark_catalog.default
> RESET spark.sql.defaultPath;

-- Error cases.
> SET PATH = spark_catalog.default, spark_catalog.default;
  [DUPLICATE_SQL_PATH_ENTRY]

> SET PATH = my_schema_no_catalog;
  [INVALID_SQL_PATH_SCHEMA_REFERENCE]

-- PATH is rejected as a value of the DEFAULT_PATH conf (would cycle).
> SET spark.sql.defaultPath = PATH, system.builtin;
  [Error: invalid value]

-- SET PATH is rejected when the feature is disabled.
> SET spark.sql.path.enabled = false;
> SET PATH = spark_catalog.default;
  [UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED]
```

### Related Statements

* [Name Resolution](sql-ref-name-resolution.html)
* [`current_path` function](sql-ref-function-current-path.html)
* [SET](sql-ref-syntax-aux-conf-mgmt-set.html)
* [RESET](sql-ref-syntax-aux-conf-mgmt-reset.html)
* [USE DATABASE](sql-ref-syntax-ddl-usedb.html)
