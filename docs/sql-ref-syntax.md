---
layout: global
title: SQL Syntax
displayTitle: SQL Syntax
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

Spark SQL is Apache Spark's module for working with structured data. The SQL Syntax section describes the SQL syntax in detail along with usage examples when applicable. This document provides a list of Data Definition and Data Manipulation Statements, as well as Data Retrieval and Auxiliary Statements.

### DDL Statements

Data Definition Statements are used to create or modify the structure of database objects in a database. Spark SQL supports the following Data Definition Statements:

 * [ALTER DATABASE](sql-ref-syntax-ddl-alter-database.html)
 * [ALTER TABLE](sql-ref-syntax-ddl-alter-table.html)
 * [ALTER VIEW](sql-ref-syntax-ddl-alter-view.html)
 * [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
 * [CREATE FUNCTION](sql-ref-syntax-ddl-create-function.html)
 * [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
 * [CREATE VIEW](sql-ref-syntax-ddl-create-view.html)
 * [DROP DATABASE](sql-ref-syntax-ddl-drop-database.html)
 * [DROP FUNCTION](sql-ref-syntax-ddl-drop-function.html)
 * [DROP TABLE](sql-ref-syntax-ddl-drop-table.html)
 * [DROP VIEW](sql-ref-syntax-ddl-drop-view.html)
 * [REPAIR TABLE](sql-ref-syntax-ddl-repair-table.html)
 * [TRUNCATE TABLE](sql-ref-syntax-ddl-truncate-table.html)
 * [USE DATABASE](sql-ref-syntax-ddl-usedb.html)

### DML Statements

Data Manipulation Statements are used to add, change, or delete data. Spark SQL supports the following Data Manipulation Statements:

 * [INSERT TABLE](sql-ref-syntax-dml-insert-table.html)
 * [INSERT OVERWRITE DIRECTORY](sql-ref-syntax-dml-insert-overwrite-directory.html)
 * [LOAD](sql-ref-syntax-dml-load.html)

### Data Retrieval Statements

Spark supports <code>SELECT</code> statement that is used to retrieve rows
from one or more tables according to the specified clauses. The full syntax
and brief description of supported clauses are explained in
[SELECT](sql-ref-syntax-qry-select.html) section. The SQL statements related
to SELECT are also included in this section. Spark also provides the
ability to generate logical and physical plan for a given query using
[EXPLAIN](sql-ref-syntax-qry-explain.html) statement.

 * [SELECT Statement](sql-ref-syntax-qry-select.html)
   * [Common Table Expression](sql-ref-syntax-qry-select-cte.html)
   * [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
   * [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
   * [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
   * [HAVING Clause](sql-ref-syntax-qry-select-having.html)
   * [Hints](sql-ref-syntax-qry-select-hints.html)
   * [Inline Table](sql-ref-syntax-qry-select-inline-table.html)
   * [File](sql-ref-syntax-qry-select-file.html)
   * [JOIN](sql-ref-syntax-qry-select-join.html)
   * [LIKE Predicate](sql-ref-syntax-qry-select-like.html)
   * [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
   * [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
   * [Set Operators](sql-ref-syntax-qry-select-setops.html)
   * [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
   * [TABLESAMPLE](sql-ref-syntax-qry-select-sampling.html)
   * [Table-valued Function](sql-ref-syntax-qry-select-tvf.html)
   * [WHERE Clause](sql-ref-syntax-qry-select-where.html)
   * [Window Function](sql-ref-syntax-qry-select-window.html)
   * [CASE Clause](sql-ref-syntax-qry-select-case.html)
   * [PIVOT Clause](sql-ref-syntax-qry-select-pivot.html)
   * [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
   * [TRANSFORM Clause](sql-ref-syntax-qry-select-transform.html)
 * [EXPLAIN](sql-ref-syntax-qry-explain.html)

### Auxiliary Statements

 * [ADD FILE](sql-ref-syntax-aux-resource-mgmt-add-file.html)
 * [ADD JAR](sql-ref-syntax-aux-resource-mgmt-add-jar.html)
 * [ANALYZE TABLE](sql-ref-syntax-aux-analyze-table.html)
 * [CACHE TABLE](sql-ref-syntax-aux-cache-cache-table.html)
 * [CLEAR CACHE](sql-ref-syntax-aux-cache-clear-cache.html)
 * [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
 * [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
 * [DESCRIBE QUERY](sql-ref-syntax-aux-describe-query.html)
 * [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
 * [LIST FILE](sql-ref-syntax-aux-resource-mgmt-list-file.html)
 * [LIST JAR](sql-ref-syntax-aux-resource-mgmt-list-jar.html)
 * [REFRESH](sql-ref-syntax-aux-cache-refresh.html)
 * [REFRESH TABLE](sql-ref-syntax-aux-cache-refresh-table.html)
 * [REFRESH FUNCTION](sql-ref-syntax-aux-cache-refresh-function.html)
 * [RESET](sql-ref-syntax-aux-conf-mgmt-reset.html)
 * [SET](sql-ref-syntax-aux-conf-mgmt-set.html)
 * [SHOW COLUMNS](sql-ref-syntax-aux-show-columns.html)
 * [SHOW CREATE TABLE](sql-ref-syntax-aux-show-create-table.html)
 * [SHOW DATABASES](sql-ref-syntax-aux-show-databases.html)
 * [SHOW FUNCTIONS](sql-ref-syntax-aux-show-functions.html)
 * [SHOW PARTITIONS](sql-ref-syntax-aux-show-partitions.html)
 * [SHOW TABLE EXTENDED](sql-ref-syntax-aux-show-table.html)
 * [SHOW TABLES](sql-ref-syntax-aux-show-tables.html)
 * [SHOW TBLPROPERTIES](sql-ref-syntax-aux-show-tblproperties.html)
 * [SHOW VIEWS](sql-ref-syntax-aux-show-views.html)
 * [UNCACHE TABLE](sql-ref-syntax-aux-cache-uncache-table.html)
