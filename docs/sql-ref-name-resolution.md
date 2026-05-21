---
layout: global
title: Name Resolution
displayTitle: Name Resolution
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

Name resolution is the process by which [identifiers](sql-ref-identifier.html) are resolved to specific column-, field-, parameter-, table-, function-, or variable-references.

## SQL Path

For unqualified references to functions, tables, views, and session variables Spark walks
an ordered list of namespaces known as the **SQL Path**. The first match along the path wins.

The path is a list of catalog-qualified schema names. In addition to ordinary persistent schemas it
can refer to two virtual system namespaces:

- `system.builtin` &mdash; the set of built-in functions provided by Spark (such as `abs`, `concat`,
  `current_user`, `current_path`, ...). Includes functions injected by `SparkSessionExtensions`.
- `system.session` &mdash; the per-session namespace that holds temporary views, temporary functions,
  and session variables created in the current session.

Both system namespaces are special: they cannot be created or dropped, and persistent objects with
these names live in different (`spark_catalog`-qualified) schemas. The 2-part shortcuts
`builtin.name` and `session.name` are accepted as synonyms for `system.builtin.name` and
`system.session.name`.

The path is observable through the [`current_path()`](sql-ref-function-current-path.html) function.

### Enabling and setting the path

The `SET PATH` statement is gated by the `spark.sql.path.enabled` configuration (default `false`).
When `false`, `SET PATH` raises `UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED`, but unqualified
resolution still walks a fixed default path and `current_path()` still returns it.

When `spark.sql.path.enabled` is `true`, you can change the path with
[`SET PATH`](sql-ref-syntax-aux-conf-mgmt-set-path.html), for example:

```sql
SET PATH = spark_catalog.analytics, spark_catalog.default, system.builtin;
```

If `SET PATH` has not been issued in the session, the effective path is the **default path**,
which is either taken from the `spark.sql.defaultPath` configuration (when set) or composed
automatically from `system.builtin`, `system.session`, and the current schema. The same
`DEFAULT_PATH` value is what `SET PATH = DEFAULT_PATH` expands to. See
[How `DEFAULT_PATH` is derived](sql-ref-syntax-aux-conf-mgmt-set-path.html#how-default_path-is-derived)
for the full derivation and how to change it.

Inside `SET PATH` the following shortcut tokens are accepted:

| Token | Expands to |
| :---- | :--------- |
| `DEFAULT_PATH` | The default path described above. |
| `SYSTEM_PATH` | `system.builtin` and `system.session`, in the configured order. |
| `PATH` | The current value of the path (useful when appending). |
| `CURRENT_SCHEMA` / `CURRENT_DATABASE` | A virtual marker that resolves to the current schema (`current_catalog.current_schema`) every time the path is consulted. |

### When the path is consulted

The path participates only in **DML** (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, ...) and in
query expressions inside DDL bodies. DDL itself &mdash; `CREATE TABLE`, `CREATE VIEW`,
`CREATE FUNCTION`, `DROP ...`, `ALTER ...`, etc. &mdash; resolves unqualified object names against the
current catalog and schema (`current_catalog.current_schema`), not the path. This is so that
`CREATE TABLE t` always creates `t` in the current schema regardless of how PATH is set.

When you create a persistent view or a SQL UDF, Spark captures the effective path at creation time
into the object's metadata. Each time the view or function is invoked its body resolves against
that **frozen path**, not the invoker's current path. Invocations of `current_schema()` and
`current_path()` inside the body still reflect the invoker's context.

### Reserved names and collisions

The SQL Path feature relies on three names being treated specially:

- **`system`** as a catalog. Spark's `system` catalog is a synthetic namespace; it serves the
  `system.builtin` and `system.session` entries and is not loadable as a v2 catalog plugin. The
  current catalog cannot be `system` and `SET PATH` does not look up `system` through the v2
  catalog API. Registering a v2 catalog under the name `system`
  (`spark.sql.catalog.system = ...`) is therefore not supported.

- **`session`** as a schema name in any catalog. Persistent schemas literally named `session` are
  allowed by the catalog API but are discouraged: the unqualified 2-part form `session.x` is
  interpreted as the synonym `system.session.x` (a temporary object) by default. To target a
  persistent schema called `session`, qualify it with the catalog name
  (`spark_catalog.session.x`).

- **`builtin`** as a schema name in any catalog. Persistent schemas literally named `builtin` are
  similarly allowed but discouraged: the unqualified 2-part form `builtin.x` is interpreted as the
  synonym `system.builtin.x`. To target a persistent schema called `builtin`, qualify it with the
  catalog name (`spark_catalog.builtin.x`).

These collisions matter only for 2-part names; 1-part lookups always go through the SQL Path, and
3-part names are never ambiguous.

Two internal configurations let advanced users tune the behavior when collisions exist; ordinary
workloads should not need to change them.

| Configuration | Purpose |
| :------------ | :------ |
| `spark.sql.legacy.persistentCatalogFirst` | When `true`, the legacy lookup order is used for partially qualified `builtin.x` and `session.x`: the persistent catalog (e.g. `spark_catalog.builtin.x`) is tried first, and only if it does not yield a match does Spark fall back to the synthetic `system.builtin.x` / `system.session.x`. Default `false` (system namespace wins). |
| `spark.sql.functionResolution.sessionOrder` | Controls where the per-session `system.session` namespace sits relative to `system.builtin` and the current persistent schema when assembling the default path. Values: `first` (session, builtin, persistent), `second` (builtin, session, persistent &mdash; default), `last` (builtin, persistent, session). Affects both `DEFAULT_PATH` expansion and the unqualified search path reported in `UNRESOLVED_ROUTINE` / `TABLE_OR_VIEW_NOT_FOUND` errors. |

## Column, field, parameter, and variable resolution

Identifiers in expressions can be references to any one of the following:

- Column name based on a view, table, common table expression (CTE), or a column_alias.
- Field name or map key within a struct or map.
  Fields and keys can never be unqualified.
- Parameter name of a SQL User Defined Function.
- Session or SQL script local variable name.
- A special function such as `current_user` or `current_date` which does not require the usage of `()`.
- The `DEFAULT` keyword which is used in the context of `INSERT`, or `SET VARIABLE` to set a column or variable value to its default.

Name resolution applies the following principles:

- The _closest_ matching reference wins, and
- Columns and parameter win over fields and keys.

In detail, resolution of identifiers to a specific reference follows these rules in order:

1. **Local references**

   1. **Column reference**

      Match the identifier, which may be qualified, to a column name in a table reference of the `FROM clause`.

      If there is more than one such match, raise an AMBIGUOUS_COLUMN_OR_FIELD error.

   1. **Parameterless function reference**

      If the identifier is unqualified and matches `current_user`, `current_date`, or `current_timestamp`: Resolve it as one of these functions.

   1. **Column DEFAULT specification**

      If the identifier is unqualified, matches `default` and makes up the entire expression in the context of an `UPDATE SET`, `INSERT VALUES`, or `MERGE WHEN [NOT] MATCHED`: Resolve as the respective `DEFAULT` value of the target table of the `INSERT`.

   1. **Struct field or map key reference**

      If the identifier is qualified, then endeavor to match it to a field or map key according to the following steps:

      A. Remove the last identifier and treat it as a field or key.

      B. Match the remainder to a column in table reference of the `FROM clause`.

         - If there is more than one such match, raise an AMBIGUOUS_COLUMN_OR_FIELD error.

         - If there is a match and the column is a:

           - **`STRUCT`**: Match the field.

             If the field cannot be matched, raise a FIELD_NOT_FOUND error.

             If there is more than one field, raise a AMBIGUOUS_COLUMN_OR_FIELD error.

           - **`MAP`**: Raise an error if the key is qualified.

             A runtime error may occur if the key is not actually present in the map.

           - Any other type: Raise an error.

      C. Repeat the preceding step to remove the trailing identifier as a field. Apply rules (A) and (B) while there is an identifier left to interpret as a column.

1. **Lateral column aliasing**

   If the expression is within a `SELECT` list, match the leading identifier to a preceding column alias in that `SELECT` list.

   If there is more than one such match, raise an AMBIGUOUS_LATERAL_COLUMN_ALIAS error.

   Match each remaining identifier as a field or a map key, and raise FIELD_NOT_FOUND or AMBIGUOUS_COLUMN_OR_FIELD error if they cannot be matched.

1. **Correlation**

   - **LATERAL**

     If the query is preceded by a `LATERAL` keyword, apply rules 1.a and 1.d considering the table references in the `FROM` containing the query and preceding the `LATERAL`.

   - **Regular**

     If the query is a scalar subquery, `IN`, or `EXISTS` subquery apply rules 1.a, 1.d, and 2 considering the table references in the containing query’s `FROM` clause.

1. **Nested correlation**

   Re-apply rule 3 iterating over the nesting levels of the query.

1. **[FOR loop](control-flow/for-stmt.md]**

   If the statement is contained in a `FOR` loop:

   A. Match the identifier to a column in a `FOR` loop statement query.
 
      If the identifier is qualified, the qualifier must match the name of the FOR loop variable if defined.

   B. If the identifier is qualified, match to a field or map key of a parameter following rule 1.c

1. **[Compound statement](control-flow/compound-stmt.html)**

   If the statement is contained in a compound statement:

   A. Match the identifier to a variable declared in that compound statement.

      If the identifier is qualified, the qualifier must match the label of the compound statement if one was defined.

   B. If the identifier is qualified, match to a field or map key of a variable following rule 1.c

1. **Nested compound statement or `FOR` loop**

   Re-apply rules 5 and 6, iterating over the nesting levels of the compound statement.

1. **Routine parameters**

   If the expression is part of a CREATE FUNCTION statement:

   1. Match the identifier to a parameter name. If the identifier is qualified, the qualifier must match the name of the routine.
   1. If the identifier is qualified, match to a field or map key of a parameter following rule 1.c

1. **Session Variables**

   1. Match the identifier to a session variable name.
      If the identifier is qualified, the qualifier must be `session` or `system.session`.
      If the identifier is unqualified, `system.session` must be present on the SQL Path
      (the default path includes it).
   1. If the identifier is qualified, match to a field or map key of a variable following rule 1.c

### Limitations

To prevent execution of potentially expensive correlated queries, Spark limits supported correlation to one level.
This restriction also applies to parameter references in SQL functions.

### Examples

```sql
-- Differentiating columns and fields
> SELECT a FROM VALUES(1) AS t(a);
 1

> SELECT t.a FROM VALUES(1) AS t(a);
 1

> SELECT t.a FROM VALUES(named_struct('a', 1)) AS t(t);
 1

-- A column takes precendece over a field
> SELECT t.a FROM VALUES(named_struct('a', 1), 2) AS t(t, a);
 2

-- Implict lateral column alias
> SELECT c1 AS a, a + c1 FROM VALUES(2) AS T(c1);
 2  4

-- A local column reference takes precedence, over a lateral column alias
> SELECT c1 AS a, a + c1 FROM VALUES(2, 3) AS T(c1, a);
 2  5

-- A scalar subquery correlation to S.c3
> SELECT (SELECT c1 FROM VALUES(1, 2) AS t(c1, c2)
           WHERE t.c2 * 2 = c3)
    FROM VALUES(4) AS s(c3);
 1

-- A local reference takes precedence over correlation
> SELECT (SELECT c1 FROM VALUES(1, 2, 2) AS t(c1, c2, c3)
           WHERE t.c2 * 2 = c3)
    FROM VALUES(4) AS s(c3);
  NULL

-- An explicit scalar subquery correlation to s.c3
> SELECT (SELECT c1 FROM VALUES(1, 2, 2) AS t(c1, c2, c3)
           WHERE t.c2 * 2 = s.c3)
    FROM VALUES(4) AS s(c3);
 1

-- Correlation from an EXISTS predicate to t.c2
> SELECT c1 FROM VALUES(1, 2) AS T(c1, c2)
    WHERE EXISTS(SELECT 1 FROM VALUES(2) AS S(c2)
                  WHERE S.c2 = T.c2);
 1

-- Attempt a lateral correlation to t.c2
> SELECT c1, c2, c3
    FROM VALUES(1, 2) AS t(c1, c2),
         (SELECT c3 FROM VALUES(3, 4) AS s(c3, c4)
           WHERE c4 = c2 * 2);
 [UNRESOLVED_COLUMN] `c2`

-- Successsful usage of lateral correlation with keyword LATERAL
> SELECT c1, c2, c3
    FROM VALUES(1, 2) AS t(c1, c2),
         LATERAL(SELECT c3 FROM VALUES(3, 4) AS s(c3, c4)
                  WHERE c4 = c2 * 2);
 1  2  3

-- Referencing a parameter of a SQL function
> CREATE OR REPLACE TEMPORARY FUNCTION func(a INT) RETURNS INT
    RETURN (SELECT c1 FROM VALUES(1) AS T(c1) WHERE c1 = a);
> SELECT func(1), func(2);
 1  NULL

-- A column takes precedence over a parameter
> CREATE OR REPLACE TEMPORARY FUNCTION func(a INT) RETURNS INT
    RETURN (SELECT a FROM VALUES(1) AS T(a) WHERE t.a = a);
> SELECT func(1), func(2);
 1  1

-- Qualify the parameter with the function name
> CREATE OR REPLACE TEMPORARY FUNCTION func(a INT) RETURNS INT
    RETURN (SELECT a FROM VALUES(1) AS T(a) WHERE t.a = func.a);
> SELECT func(1), func(2);
 1  NULL

-- Lateral alias takes precedence over correlated reference
> SELECT (SELECT c2 FROM (SELECT 1 AS c1, c1 AS c2) WHERE c2 > 5)
    FROM VALUES(6) AS t(c1)
  NULL

-- Lateral alias takes precedence over function parameters
> CREATE OR REPLACE TEMPORARY FUNCTION func(x INT)
    RETURNS TABLE (a INT, b INT, c DOUBLE)
    RETURN SELECT x + 1 AS x, x
> SELECT * FROM func(1)
  2 2

-- All together now
> CREATE OR REPLACE TEMPORARY VIEW lat(a, b) AS VALUES('lat.a', 'lat.b');

> CREATE OR REPLACE TEMPORARY VIEW frm(a) AS VALUES('frm.a');

> CREATE OR REPLACE TEMPORARY FUNCTION func(a INT, b int, c int)
  RETURNS TABLE
  RETURN SELECT t.*
    FROM lat,
         LATERAL(SELECT a, b, c
                   FROM frm) AS t;

> VALUES func('func.a', 'func.b', 'func.c');
  a      b      c
  -----  -----  ------
  frm.a  lat.b  func.c
```

## Table and view resolution

An identifier in a table reference can be any of the following:

- Persistent table or view
- Common table expression (CTE)
- [Temporary view](sql-ref-syntax-ddl-create-view.html)

Resolution of an identifier depends on whether it is qualified:

- **Qualified**

  If the identifier is fully qualified with three parts: `catalog.schema.relation`, it is unique.

  If the identifier consists of two parts: `schema.relation`, it is further qualified with the
  result of `SELECT current_catalog()` to make it unique.
  As a special case, the schema `session` is implicitly qualified with the catalog `system` and
  interpreted as a temporary view.

  If the identifier is `system.session.relation`, it targets the temporary view scope only.

- **Unqualified**

  1. **Common table expression**

     If the reference is within the scope of a `WITH` clause, match the identifier to a CTE
     starting with the immediately containing `WITH` clause and moving outwards from there.

  1. **SQL Path walk**

     For each entry on the [SQL Path](#sql-path) in order:

     - When the entry is `system.session`, attempt to match the identifier as a temporary view.
     - Otherwise, fully qualify the identifier with the entry (`catalog.schema.relation`) and look
       it up as a persistent relation.

     The first match wins. If no entry yields a match, the relation is unresolved.

If the relation cannot be resolved to any table, view, or CTE, Spark raises a
`TABLE_OR_VIEW_NOT_FOUND` error. The error includes the effective search path, for example
`searchPath = [system.builtin, system.session, spark_catalog.default]`.

> Note: persistent views capture their creation-time SQL Path. When a persistent view is
> referenced, the body resolves against the frozen path rather than the invoker's current path;
> see [SQL Path](#sql-path).

### Examples

```sql
-- Setting up a scenario
> USE CATALOG spark_catalog;
> USE SCHEMA default;

> CREATE TABLE rel(c1 int);
> INSERT INTO rel VALUES(1);

-- An fully qualified reference to rel:
> SELECT c1 FROM spark_catalog.default.rel;
 1

-- A partially qualified reference to rel:
> SELECT c1 FROM default.rel;
 1

-- An unqualified reference to rel:
> SELECT c1 FROM rel;
 1

-- Add a temporary view with a conflicting name:
> CREATE TEMPORARY VIEW rel(c1) AS VALUES(2);

-- For unqualified references the temporary view takes precedence over the persisted table:
> SELECT c1 FROM rel;
 2

-- A temporary view can be qualified with `session` or `system.session`:
> SELECT c1 FROM session.rel;
 2
> SELECT c1 FROM system.session.rel;
 2

-- Other 2-part qualifications resolve to the persisted table:
> SELECT c1 FROM default.rel;
 1

-- An unqualified reference to a common table expression wins even over a temporary view:
> WITH rel(c1) AS (VALUES(3))
    SELECT * FROM rel;
 3

-- If CTEs are nested, the match nearest to the table reference takes precedence.
> WITH rel(c1) AS (VALUES(3))
    (WITH rel(c1) AS (VALUES(4))
      SELECT * FROM rel);
  4

-- To resolve the table instead of the CTE, qualify it:
> WITH rel(c1) AS (VALUES(3))
    (WITH rel(c1) AS (VALUES(4))
      SELECT * FROM default.rel);
  1

-- For a CTE to be visible it must contain the query
> SELECT * FROM (WITH cte(c1) AS (VALUES(1))
                   SELECT 1),
                cte;
  [TABLE_OR_VIEW_NOT_FOUND] The table or view `cte` cannot be found.

-- PATH drives unqualified relation lookup order
> SET spark.sql.path.enabled = true;
> CREATE SCHEMA db_a;
> CREATE SCHEMA db_b;
> CREATE TABLE db_a.t USING parquet AS SELECT 1 AS v;
> CREATE TABLE db_b.t USING parquet AS SELECT 2 AS v;

> SET PATH = spark_catalog.db_a, spark_catalog.db_b, system.builtin;
> SELECT v FROM t;
 1

> SET PATH = spark_catalog.db_b, spark_catalog.db_a, system.builtin;
> SELECT v FROM t;
 2

-- Three-part `system.session.x` references the temporary scope only:
> SELECT * FROM system.session.no_such_view;
  [TABLE_OR_VIEW_NOT_FOUND] ... `system`.`session`.`no_such_view` ...
```

## Function resolution

A function reference is recognized by the mandatory trailing set of parentheses.

It can resolve to:

- A built-in function provided by Spark or a `SparkSessionExtensions` injection
  (`system.builtin`),
- A temporary user-defined function scoped to the current session (`system.session`), or
- A persistent user-defined function stored in a catalog schema.

Resolution of a function name depends on whether it is qualified:

- **Qualified**

  If the name is fully qualified with three parts: `catalog.schema.function`, it is unique.

  If the name consists of two parts: `schema.function`, it is further qualified with the result of
  `SELECT current_catalog()` to make it unique. As a special case, the 2-part shortcuts
  `builtin.function` and `session.function` are accepted as synonyms for `system.builtin.function`
  and `system.session.function` respectively.

  The function is then looked up in the resulting namespace.

- **Unqualified**

  For unqualified function names Spark walks the [SQL Path](#sql-path) and returns the first match:

  1. For each entry on the path in order:

     - When the entry is `system.builtin`, attempt to match against the set of built-in functions.
     - When the entry is `system.session`, attempt to match against temporary functions.
     - Otherwise, fully qualify the function name with the entry (`catalog.schema.function`) and
       look it up as a persistent function.

  2. The first match wins. If no entry yields a match, the function is unresolved.

If the function cannot be resolved Spark raises an `UNRESOLVED_ROUTINE` error. The error includes
the effective search path, for example
`searchPath = [system.builtin, system.session, spark_catalog.default]`.

> Note: SQL user-defined functions (`CREATE FUNCTION`) capture their creation-time SQL Path. When
> the function is invoked, the body resolves against the frozen path rather than the invoker's
> current path; see [SQL Path](#sql-path). Inside the body, `current_schema()` and
> `current_path()` still reflect the invoker's context.

### Examples

```sql
> USE CATALOG spark_catalog;
> USE SCHEMA default;

-- Create a function with the same name as a builtin
> CREATE FUNCTION concat(a STRING, b STRING) RETURNS STRING
    RETURN b || a;

-- unqualified reference resolves to the builtin CONCAT
> SELECT concat('hello', 'world');
 helloworld

-- Qualified reference resolves to the persistent function
> SELECT default.concat('hello', 'world');
 worldhello

-- Create a persistent function
> CREATE FUNCTION func(a INT, b INT) RETURNS INT
    RETURN a + b;

-- The persistent function is resolved without qualifying it
> SELECT func(4, 2);
 6

-- Create a conflicting temporary function
> CREATE FUNCTION func(a INT, b INT) RETURNS INT
    RETURN a / b;

-- The temporary function takes precedent
> SELECT func(4, 2);
 2

-- To resolve the persistent function it now needs qualification
> SELECT spark_catalog.default.func(4, 3);
 6

-- A built-in can always be reached by qualification, even when shadowed
> CREATE TEMPORARY FUNCTION abs() RETURNS INT RETURN 999;
> SELECT abs(-5);
 5
> SELECT session.abs();
 999
> SELECT builtin.abs(-5);
 5
> SELECT system.builtin.abs(-5);
 5
> DROP TEMPORARY FUNCTION abs;

-- PATH controls unqualified routine lookup order
> SET spark.sql.path.enabled = true;
> CREATE SCHEMA path_a;
> CREATE SCHEMA path_b;
> CREATE FUNCTION path_a.pick() RETURNS INT RETURN 10;
> CREATE FUNCTION path_b.pick() RETURNS INT RETURN 20;

> SET PATH = spark_catalog.path_a, spark_catalog.path_b, system.builtin;
> SELECT pick();
 10

> SET PATH = spark_catalog.path_b, spark_catalog.path_a, system.builtin;
> SELECT pick();
 20

-- Unresolved routine lists the effective search path
> SET PATH = spark_catalog.default, system.builtin;
> SELECT does_not_exist();
  [UNRESOLVED_ROUTINE] ... searchPath: [`spark_catalog`.`default`, `system`.`builtin`] ...
```
