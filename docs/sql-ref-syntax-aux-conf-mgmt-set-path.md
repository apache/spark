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

`SET PATH` changes the **SQL Path** of the current session.

The SQL Path is an ordered list of catalog-qualified schema names that Spark walks when
resolving unqualified references to functions, tables, views, and session variables in queries
and DML (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`). The first match wins. DDL
(`CREATE TABLE`, `CREATE VIEW`, `CREATE FUNCTION`, `DROP`, `ALTER`, ...) resolves unqualified
object names against `current_catalog.current_schema`, not the path; so `CREATE TABLE t` always
creates `t` in the current schema regardless of the path.

The path can include two virtual namespaces in the `system` catalog:

- `system.builtin` &mdash; built-in functions, including those injected by
  `SparkSessionExtensions`.
- `system.session` &mdash; temporary views, temporary functions, and session variables in the
  current session.

`SET PATH` is controlled by `spark.sql.path.enabled`. When it is `false` (the default),
`SET PATH` raises `UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED`. Unqualified resolution and
[`current_path()`](sql-ref-function-current-path.html) still use the default path.

The initial value of `PATH` in a session is `DEFAULT_PATH`. `DEFAULT_PATH` is either the value of
`spark.sql.defaultPath`, or, when that configuration is empty, a built-in value composed of
`system.builtin`, `system.session`, and the current schema. To override, set
`spark.sql.defaultPath`. See the [`DEFAULT_PATH` parameter](#parameters) for the exact derivation
rules.

The effect of `SET PATH` is scoped to the current session and is lost when the session ends. To
re-apply the current default path mid-session, run `SET PATH = DEFAULT_PATH`. (This stores a
snapshot of `DEFAULT_PATH` at the moment of the statement; later changes to
`spark.sql.defaultPath` are not picked up automatically.) Cloned sessions inherit the parent's
path at clone time; later changes in the child do not propagate back.

Persistent views and SQL UDFs capture the path at `CREATE` time into the object's metadata.
Each invocation resolves the body against that frozen path, not the invoker's current path;
`current_schema()` and `current_path()` inside the body still return the invoker's context.

The leading names `session` and `builtin` have special meaning in 2-part references; see
[Reserved system names](sql-ref-identifier.html#reserved-system-names).

### Syntax

```sql
SET PATH = path_element [ , ... ]

path_element
    { DEFAULT_PATH |
      SYSTEM_PATH |
      PATH |
      CURRENT_SCHEMA |
      CURRENT_DATABASE |
      catalog_name . namespace [ . namespace ... ] }
```

### Parameters

* **`DEFAULT_PATH`**

  Expands to the session's default path. The default path has two layers:

  1. If `spark.sql.defaultPath` is set to a non-empty value, that value is parsed using the same
     grammar as `SET PATH` (with one restriction: the `PATH` keyword is not allowed inside the
     conf value, since it would be self-referential).

     The conf value is validated for syntax at the time it is set; an invalid value is rejected.
     Static duplicates inside the conf are tolerated (unlike interactive `SET PATH`, which
     rejects them) so a later `USE SCHEMA` cannot turn a previously valid default into a runtime
     error. A `DEFAULT_PATH` token inside the conf value resolves to the spark-built-in default
     below to avoid a cycle, rather than recursing.

  2. If `spark.sql.defaultPath` is empty (the factory setting), the spark-built-in default
     applies: `system.builtin`, `system.session`, and the current schema
     (`current_catalog.current_schema`), in that order.

  To change the default path, set `spark.sql.defaultPath` via any of the usual mechanisms
  (`SET spark.sql.defaultPath = ...` at runtime, `--conf` on `spark-submit`, `SparkConf`, or
  `spark-defaults.conf`); clear it with `RESET spark.sql.defaultPath` to return to the
  spark-built-in default.

* **`SYSTEM_PATH`**

  Expands to the system-managed namespaces under the `system` catalog. Today this is just
  `system.builtin`, but it is reserved for future system-managed schemas (for example, hosting
  built-in AI, geospatial, or ML functions).

* **`PATH`**

  Expands to the **current** value of the SQL Path. Useful for appending entries without
  re-typing them, for example `SET PATH = PATH, spark_catalog.analytics`.
  `PATH` is not allowed in the value of `spark.sql.defaultPath` (it would create a cycle).

* **`CURRENT_SCHEMA`** / **`CURRENT_DATABASE`**

  A virtual marker that resolves to the catalog-qualified current schema
  (`current_catalog.current_schema`) every time the path is consulted. This means subsequent
  `USE SCHEMA` statements are picked up without re-issuing `SET PATH`.
  `CURRENT_DATABASE` is a synonym for `CURRENT_SCHEMA`.

* **`catalog_name . namespace [ . namespace ... ]`**

  An explicit catalog-qualified namespace reference (`catalog.schema` or, for catalogs with
  multi-level namespaces, `catalog.ns1.ns2...`). At least two parts are required.
  The catalog and namespace do not need to exist at the time of `SET PATH`; non-existent entries
  are silently skipped during name resolution.

  Identifier quoting follows the usual rules. Backtick-quoted parts that contain a dot are
  preserved, for example ``spark_catalog.`sch.b` ``.

### Semantics

* Setting the path takes effect immediately.
* Identifier case is preserved in storage and in `current_path()` output.
* Duplicate entries are detected after expansion and raise `DUPLICATE_SQL_PATH_ENTRY`.
  Comparisons honor the session's case sensitivity setting. Because `CURRENT_DATABASE` is an
  alias for `CURRENT_SCHEMA`, listing both is flagged as a duplicate.

### Error conditions

| Condition | Cause |
| :-------- | :---- |
| `UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED` | `SET PATH` was issued while `spark.sql.path.enabled` is `false`. |
| `INVALID_SQL_PATH_SCHEMA_REFERENCE` | An entry with fewer than two parts was given. |
| `DUPLICATE_SQL_PATH_ENTRY` | Two entries collapsed to the same concrete namespace after expansion. |

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
> SELECT current_path();
 system.builtin,system.session,spark_catalog.default
> SET PATH = SYSTEM_PATH;
> SELECT current_path();
 system.builtin

-- SYSTEM_PATH composes naturally with the working schema.
> SET PATH = SYSTEM_PATH, CURRENT_SCHEMA;
> SELECT current_path();
 system.builtin,spark_catalog.default

-- Append an entry by referring to the current path.
> SET PATH = spark_catalog.default, system.builtin;
> SET PATH = PATH, spark_catalog.analytics;
> SELECT current_path();
 spark_catalog.default,system.builtin,spark_catalog.analytics

-- CURRENT_SCHEMA is re-evaluated each time; USE SCHEMA updates the effective path.
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

-- Append a schema of shared UDFs so callers do not have to qualify them.
> CREATE SCHEMA spark_catalog.shared_udfs;
> CREATE FUNCTION spark_catalog.shared_udfs.to_iso_date(d DATE) RETURNS STRING
    RETURN date_format(d, 'yyyy-MM-dd');
> SET PATH = PATH, spark_catalog.shared_udfs;
> SELECT to_iso_date(DATE'2026-05-22');
 2026-05-22

-- Drop system.session from the path to force temporary objects to be qualified explicitly.
> CREATE TEMPORARY FUNCTION revenue() RETURNS INT RETURN 42;
> SELECT revenue();                  -- resolves via the default path
 42
> SET PATH = system.builtin, current_schema;
> SELECT revenue();                  -- now must be qualified
 [UNRESOLVED_ROUTINE] `revenue` ...
> SELECT session.revenue();
 42

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
