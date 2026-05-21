---
layout: global
title: CREATE VIEW
displayTitle: CREATE VIEW 
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

Views are based on the result-set of an `SQL` query. `CREATE VIEW` constructs
a virtual table that has no physical data therefore other operations like
`ALTER VIEW` and `DROP VIEW` only change metadata. 

### Syntax

```sql
CREATE [ OR REPLACE ] [ [ GLOBAL ] TEMPORARY ] VIEW [ IF NOT EXISTS ] view_identifier
    create_view_clauses AS query
```

### Parameters

* **OR REPLACE**

    If a view of same name already exists, it will be replaced.

* **[ GLOBAL ] TEMPORARY**

    `TEMPORARY` views are session-scoped and are dropped when the session ends;
    no entry is persisted in the underlying metastore.
    Temporary views live in the per-session `system.session` namespace.

    `GLOBAL TEMPORARY` views are tied to the system-preserved temporary database `global_temp`.

* **IF NOT EXISTS**

    Creates a view if it does not exist.
    This clause is not supported for `TEMPORARY` views yet.

* **view_identifier**

    Specifies a view name.

    * For a **persistent** view the name may be optionally qualified with a database name (or a
      catalog and database). If the name is not qualified the view is created in the current
      schema.

      **Syntax:** `[ catalog_name. ] [ database_name. ] view_name`

    * For a **temporary** view the name may be optionally qualified with the session schema
      (`session` or `system.session`). Any other qualifier is rejected with
      `INVALID_TEMP_OBJ_QUALIFIER`. For example, `CREATE TEMPORARY VIEW session.v ...` and
      `CREATE TEMPORARY VIEW system.session.v ...` are accepted; `CREATE TEMPORARY VIEW mydb.v ...`
      is not.

      **Syntax:** `[ { session | system.session } . ] view_name`

    The fully qualified view name must be unique within its schema.

> Note: a persistent view captures the SQL Path that was in effect when `CREATE VIEW` ran. When the
> view is referenced, the body resolves against that frozen path, not the invoker's current path.
> See [Name Resolution](sql-ref-name-resolution.html#sql-path).
> Use [DESCRIBE EXTENDED](sql-ref-syntax-aux-describe-table.html) to inspect the captured path.

* **create_view_clauses**

    These clauses are optional and order insensitive. It can be of following formats.

    * `[ ( column_name [ COMMENT column_comment ], ... ) ]` to specify column-level comments.
    * `[ COMMENT view_comment ]` to specify view-level comments.
    * `[ TBLPROPERTIES ( property_name = property_value [ , ... ] ) ]` to add metadata key-value pairs.
    * `[ WITH SCHEMA { BINDING | COMPENSATION | [ TYPE ] EVOLUTION } ]` to specify how the view reacts to schema changes

      This clause is not supported for `TEMPORARY` views.

      * **BINDING** - The view can tolerate only type changes in the underlying schema requiring safe up-casts.
      * **COMPENSATION** - The view can tolerate type changes in the underlying schema requiring casts. Runtime casting errors may occur.
      * **TYPE EVOLUTION** - The view will adapt to any type changes in the underlying schema.
      * **EVOLUTION** - For views defined without a column lists any schema changes are adapted by the view, including, for queries with `SELECT *` dropped or added columns.
        If the view is defined with a column list, the clause is interpreted as `TYPE EVOLUTION`.
      
      The default is `WITH SCHEMA COMPENSATION`.

* **query**
  A [SELECT](sql-ref-syntax-qry-select.html) statement that constructs the view from base tables or other views.

### Examples

```sql
-- Create or replace view for `experienced_employee` with comments.
CREATE OR REPLACE VIEW experienced_employee
    (ID COMMENT 'Unique identification number', Name) 
    COMMENT 'View for experienced employees'
    AS SELECT id, name FROM all_employee
        WHERE working_years > 5;

-- Create a global temporary view `subscribed_movies`.
CREATE GLOBAL TEMPORARY VIEW subscribed_movies 
    AS SELECT mo.member_id, mb.full_name, mo.movie_title
        FROM movies AS mo INNER JOIN members AS mb 
        ON mo.member_id = mb.id;

-- Create a view filtering the `orders` table which will adjust to schema changes in `orders`.
CREATE OR REPLACE VIEW open_orders WITH SCHEMA EVOLUTION
    AS SELECT * FROM orders WHERE status = 'open';
```

### Create a temporary view with a session qualifier

```sql
-- Unqualified, `session`-qualified, and `system.session`-qualified names all create the same
-- temporary view in the per-session `system.session` namespace.
CREATE TEMPORARY VIEW recent_orders
    AS SELECT * FROM orders WHERE order_date > current_date - INTERVAL 7 DAYS;

CREATE OR REPLACE TEMPORARY VIEW session.recent_orders
    AS SELECT * FROM orders WHERE order_date > current_date - INTERVAL 7 DAYS;

CREATE OR REPLACE TEMPORARY VIEW system.session.recent_orders
    AS SELECT * FROM orders WHERE order_date > current_date - INTERVAL 7 DAYS;

-- All three names address the same temporary view:
SELECT count(*) FROM recent_orders;
SELECT count(*) FROM session.recent_orders;
SELECT count(*) FROM system.session.recent_orders;

-- DROP VIEW accepts the same qualifiers (there is no DROP TEMPORARY VIEW form):
DROP VIEW session.recent_orders;

-- Any other qualifier on a TEMPORARY view is rejected.
CREATE TEMPORARY VIEW mydb.bad_temp AS SELECT 1;
  [INVALID_TEMP_OBJ_QUALIFIER] qualifier `mydb` is not allowed for temporary VIEW ...

CREATE TEMPORARY VIEW system.builtin.bad_temp AS SELECT 1;
  [INVALID_TEMP_OBJ_QUALIFIER] qualifier `system`.`builtin` is not allowed for temporary VIEW ...
```

### Frozen SQL Path

A persistent view captures the SQL Path that is in effect at `CREATE VIEW` time. The view body
resolves against that frozen path on every reference, even when the caller's session has set a
different PATH. See [Name Resolution](sql-ref-name-resolution.html#sql-path).

```sql
> SET spark.sql.path.enabled = true;

> CREATE SCHEMA views_a;
> CREATE SCHEMA views_b;
> CREATE TABLE views_a.t USING parquet AS SELECT 1 AS id;
> CREATE TABLE views_b.t USING parquet AS SELECT 2 AS id;

-- The PATH at CREATE VIEW time points at views_a, so unqualified `t` in the view body binds to
-- views_a.t.
> SET PATH = spark_catalog.views_a, system.builtin;
> CREATE VIEW default.v_frozen AS SELECT id FROM t;

-- Flip the live PATH. The view body still resolves `t` against the frozen path.
> SET PATH = spark_catalog.views_b, system.builtin;

-- A bare query follows the LIVE path:
> SELECT id FROM t;
 2

-- The view body follows its FROZEN path:
> SELECT id FROM default.v_frozen;
 1

-- DESCRIBE EXTENDED shows the captured path:
> DESCRIBE EXTENDED default.v_frozen;
 ...
 SQL Path  spark_catalog.views_a, system.builtin
```

### Related Statements

* [ALTER VIEW](sql-ref-syntax-ddl-alter-view.html)
* [DROP VIEW](sql-ref-syntax-ddl-drop-view.html)
* [SHOW VIEWS](sql-ref-syntax-aux-show-views.html)
* [SET PATH](sql-ref-syntax-aux-conf-mgmt-set-path.html)
* [Name Resolution](sql-ref-name-resolution.html)
