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

### Syntax

```sql
DROP VIEW [ IF EXISTS ] view_identifier
```

### Parameter

* **IF EXISTS**

    If specified, no exception is thrown when the view does not exist.

* **view_identifier**

    Specifies the view name to be dropped. The view name may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] view_name`

### Examples

```sql
-- Assumes a view named `employeeView` exists.
DROP VIEW employeeView;

-- Assumes a view named `employeeView` exists in the `userdb` database
DROP VIEW userdb.employeeView;

-- Assumes a view named `employeeView` does not exist.
-- Throws exception
DROP VIEW employeeView;
Error: org.apache.spark.sql.AnalysisException: Table or view not found: employeeView;
(state=,code=0)

-- Assumes a view named `employeeView` does not exist,Try with IF EXISTS
-- this time it will not throw exception
DROP VIEW IF EXISTS employeeView;
```

### Related Statements

* [CREATE VIEW](sql-ref-syntax-ddl-create-view.html)
* [ALTER VIEW](sql-ref-syntax-ddl-alter-view.html)
* [SHOW VIEWS](sql-ref-syntax-aux-show-views.html)
* [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
* [DROP DATABASE](sql-ref-syntax-ddl-drop-database.html)
