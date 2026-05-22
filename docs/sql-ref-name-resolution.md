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

      If the identifier is unqualified and matches `current_user`, `current_date`, `current_time`, `current_timestamp`, or `current_path`: Resolve it as one of these functions.

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
      If the identifier is unqualified, `system.session` must be present on the
      [SQL Path](sql-ref-syntax-aux-conf-mgmt-set-path.html) (the default path includes it).
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

## Object name resolution

Tables, views, and functions follow the same resolution rule. It depends on how many parts the
identifier has.

### Fully qualified (3 parts) &mdash; `catalog.schema.object`

The reference is unique and is looked up in `catalog.schema`. `system.builtin.object` identifies
a built-in function; `system.session.object` identifies a temporary view, function, or session
variable.

### Partially qualified (2 parts) &mdash; `schema.object`

The identifier is qualified with `current_catalog` &mdash; producing
`current_catalog.schema.object` &mdash; unless the leading part is `session` (or `builtin`, for
functions). In that case Spark uses the
[mini-path](sql-ref-identifier.html#reserved-system-names) to choose the implicit catalog,
returning the first match:

| `spark.sql.legacy.persistentCatalogFirst` | Mini-path tried in order |
| :-------------------------------------- | :----------------------- |
| `false` (default) | the system namespace (`system.session.x` / `system.builtin.x`), then the current catalog's `session.x` / `builtin.x` |
| `true` (legacy)   | the current catalog's `session.x` / `builtin.x`, then the system namespace (`system.session.x` / `system.builtin.x`) |

### Unqualified (1 part) &mdash; `object`

In queries and DML, Spark walks the [SQL Path](sql-ref-syntax-aux-conf-mgmt-set-path.html) and
returns the first match. In DDL, the identifier is qualified with `current_catalog.current_schema`.

> Note: persistent views and SQL UDFs capture the SQL Path at `CREATE` time. When the view or
> function is invoked, its body resolves names &mdash; tables, views, and functions &mdash;
> against that frozen path, not the invoker's current path. `current_schema()` and
> `current_path()` inside the body still return the invoker's context. See
> [SET PATH](sql-ref-syntax-aux-conf-mgmt-set-path.html).

## Table and view resolution

A table reference can be a persistent table or view, a temporary view, or a common table
expression (CTE).

Resolution follows [Object name resolution](#object-name-resolution), with one addition for
unqualified references: when the reference is inside a `WITH` clause, Spark first matches the
identifier against CTEs from the innermost `WITH` outward. If no CTE matches, Spark walks the
SQL Path.

If the relation cannot be resolved, Spark raises `TABLE_OR_VIEW_NOT_FOUND`. The error includes
the effective search path, for example
`searchPath = [system.builtin, system.session, spark_catalog.default]`.

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

A function reference is recognized by the trailing parentheses, and follows
[Object name resolution](#object-name-resolution).

If the function cannot be resolved, Spark raises `UNRESOLVED_ROUTINE`. The error includes the
effective search path, for example
`searchPath = [system.builtin, system.session, spark_catalog.default]`.

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
