---
layout: global
title: DROP VIEW
displayTitle: DROP VIEW 
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

`DROP VIEW` removes the metadata associated with a specified view from the catalog.
`DROP TEMPORARY VIEW` removes only a local or global temporary view and never removes a
persistent view.

### Syntax

```sql
DROP [ TEMPORARY ] VIEW [ IF EXISTS ] view_identifier
```

### Parameter

* **IF EXISTS**

    If specified, no exception is thrown when the targeted view does not exist. For
    `DROP TEMPORARY VIEW`, this only suppresses a missing temporary view; a persistent view with
    the same name is not dropped.

* **TEMPORARY**

    Restricts the command to temporary views. An unqualified name targets a local temporary view.
    The `session` and `system.session` qualifiers also target a local temporary view. The configured
    global temporary database name targets a global temporary view; its default is `global_temp`
    and it is controlled by `spark.sql.globalTempDatabase`. Any other qualifier raises
    `INVALID_TEMP_OBJ_QUALIFIER`.

* **view_identifier**

    Specifies the view name to be dropped. The name may be optionally qualified with a database
    name (or a catalog and database). A name qualified with `session` or `system.session`
    targets a temporary view. When `TEMPORARY` is specified, only the qualifiers described above
    are allowed.

    **Syntax:** `[ catalog_name. ] [ database_name. ] view_name`

### Examples

```sql
-- Assumes a view named `employeeView` exists.
DROP VIEW employeeView;

-- Assumes a view named `employeeView` exists in the `userdb` database
DROP VIEW userdb.employeeView;

-- Assumes a view named `employeeView` does not exist.
-- Throws exception
DROP VIEW employeeView;
Error: TABLE_OR_VIEW_NOT_FOUND

-- Assumes a view named `employeeView` does not exist,Try with IF EXISTS
-- this time it will not throw exception
DROP VIEW IF EXISTS employeeView;

-- A temporary view that shadows a persistent view with the same name.
-- An unqualified DROP VIEW drops the temporary view first; qualifying with `session`
-- always targets the temporary view explicitly.
CREATE VIEW default.recent_orders AS SELECT * FROM orders WHERE order_date > current_date - 7;
CREATE TEMPORARY VIEW recent_orders AS SELECT * FROM orders WHERE order_date = current_date;

DROP VIEW session.recent_orders;             -- drops the temporary view
DROP VIEW default.recent_orders;             -- drops the persistent view

-- Drop only temporary views. The persistent view remains after the local temporary view is gone.
CREATE TEMPORARY VIEW recent_orders AS SELECT * FROM orders WHERE order_date = current_date;
DROP TEMPORARY VIEW recent_orders;
DROP TEMPORARY VIEW IF EXISTS recent_orders; -- does not drop default.recent_orders

-- Drop a global temporary view. `global_temp` is configurable with
-- `spark.sql.globalTempDatabase`.
CREATE GLOBAL TEMPORARY VIEW active_orders AS SELECT * FROM orders WHERE status = 'active';
DROP TEMPORARY VIEW global_temp.active_orders;
```

### Related Statements

* [CREATE VIEW](sql-ref-syntax-ddl-create-view.html)
* [ALTER VIEW](sql-ref-syntax-ddl-alter-view.html)
* [SHOW VIEWS](sql-ref-syntax-aux-show-views.html)
* [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
* [DROP DATABASE](sql-ref-syntax-ddl-drop-database.html)
