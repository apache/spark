---
layout: global
title: Data Retrieval
displayTitle: Data Retrieval
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

Spark supports <code>SELECT</code> statement that is used to retrieve rows
from one or more tables according to the specified clauses. The full syntax
and brief description of supported clauses are explained in
[SELECT](sql-ref-syntax-qry-select.html) section. The SQL statements related
to SELECT are also included in this section. Spark also provides the
ability to generate logical and physical plan for a given query using
[EXPLAIN](sql-ref-syntax-qry-explain.html) statement.

* [SELECT Statement](sql-ref-syntax-qry-select.html)
  * [WHERE Clause](sql-ref-syntax-qry-select-where.html)
  * [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
  * [HAVING Clause](sql-ref-syntax-qry-select-having.html)
  * [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
  * [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
  * [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
  * [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
  * [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
  * [Common Table Expression](sql-ref-syntax-qry-select-cte.html)
  * [Hints](sql-ref-syntax-qry-select-hints.html)
  * [Inline Table](sql-ref-syntax-qry-select-inline-table.html)
  * [File](sql-ref-syntax-qry-select-file.html)
  * [JOIN](sql-ref-syntax-qry-select-join.html)
  * [LIKE Predicate](sql-ref-syntax-qry-select-like.html)
  * [Set Operators](sql-ref-syntax-qry-select-setops.html)
  * [TABLESAMPLE](sql-ref-syntax-qry-select-sampling.html)
  * [Table-valued Function](sql-ref-syntax-qry-select-tvf.html)
  * [Window Function](sql-ref-syntax-qry-select-window.html)
  * [CASE Clause](sql-ref-syntax-qry-select-case.html)
  * [PIVOT Clause](sql-ref-syntax-qry-select-pivot.html)
  * [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
  * [TRANSFORM Clause](sql-ref-syntax-qry-select-transform.html)
* [EXPLAIN Statement](sql-ref-syntax-qry-explain.html)
