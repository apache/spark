---
layout: global
title: LATERAL SUBQUERY
displayTitle: LATERAL SUBQUERY
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

`LATERAL SUBQUERY` is a subquery that is preceded by the keyword `LATERAL`. It provides a way to reference columns in the preceding `FROM` clause.
Without the `LATERAL` keyword, subqueries can only refer to columns in the outer query, but not in the `FROM` clause. `LATERAL SUBQUERY` makes the complicated
queries simpler and more efficient.

### Syntax

```sql
[ LATERAL ] primary_relation [ join_relation ]
```

### Parameters

* **primary_relation**

  Specifies the primary relation. It can be one of the following:
  * Table relation
  * Aliased query

    Syntax: `( query ) [ [ AS ] alias ]`
  * Aliased relation

    Syntax: `( relation ) [ [ AS ] alias ]`
  * [Table-value function](sql-ref-syntax-qry-select-tvf.html)
  * [Inline table](sql-ref-syntax-qry-select-inline-table.html)


* **join_relation**

    Specifies a [Join relation](sql-ref-syntax-qry-select-join.html).

### Examples

```sql
CREATE TABLE t1 (c1 INT, c2 INT);
INSERT INTO t1 VALUES (0, 1), (1, 2);

CREATE TABLE t2 (c1 INT, c2 INT);
INSERT INTO t2 VALUES (0, 2), (0, 3);

SELECT * FROM t1,
  LATERAL (SELECT * FROM t2 WHERE t1.c1 = t2.c1);
+--------+-------+--------+-------+
|  t1.c1 | t1.c2 |  t2.c1 | t2.c2 |
+-------+--------+--------+-------+
|    0   |   1   |    0   |   3   |
|    0   |   1   |    0   |   2   |
+-------+--------+--------+-------+

SELECT a, b, c FROM t1,
  LATERAL (SELECT c1 + c2 AS a),
  LATERAL (SELECT c1 - c2 AS b),
  LATERAL (SELECT a * b AS c);
+--------+-------+--------+
|    a   |   b   |    c   |
+-------+--------+--------+
|    3   |  -1   |   -3   |
|    1   |  -1   |   -1   |
+-------+--------+--------+
```

### Related Statements

* [SELECT Main](sql-ref-syntax-qry-select.html)
* [JOIN](sql-ref-syntax-qry-select-join.html)
