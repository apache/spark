---
layout: global
title: JOIN
displayTitle: JOIN
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

A SQL join is used to combine rows from two relations based on join criteria. The following section describes the overall join syntax and the sub-sections cover different types of joins along with examples.

### Syntax

```sql
relation { [ join_type ] JOIN [ LATERAL ] relation [ join_criteria | nearest_by_clause ] | NATURAL join_type JOIN [ LATERAL ] relation }
```

### Parameters

* **relation**

    Specifies the relation to be joined.

* **join_type**

    Specifies the join type.

    **Syntax:**

    `[ INNER ] | CROSS | LEFT [ OUTER ] | [ LEFT ] SEMI | RIGHT [ OUTER ] | FULL [ OUTER ] | [ LEFT ] ANTI`

* **join_criteria**

    Specifies how the rows from one relation will be combined with the rows of another relation.

    **Syntax:** `ON boolean_expression | USING ( column_name [ , ... ] )`

    `boolean_expression`

    Specifies an expression with a return type of boolean.

* **nearest_by_clause**

    Specifies a nearest-by top-K ranking join. For each row on the left (query side), returns up to `num_results` rows from the right (base side), ranked by `ranking_expression`. Only `INNER` (the default) and `LEFT OUTER` join types are supported with this clause.

    **Syntax:** `{ APPROX | EXACT } NEAREST [ num_results ] BY { DISTANCE | SIMILARITY } ranking_expression`

    `APPROX | EXACT`

    Controls the search algorithm contract. `APPROX` allows the optimizer to use faster approximate strategies (such as indexed nearest-neighbor search when available). `EXACT` forces brute-force evaluation and requires `ranking_expression` to be deterministic.

    `num_results`

    A positive integer literal between 1 and 100000 that limits the number of matches per left row. Defaults to 1 when omitted.

    `DISTANCE | SIMILARITY`

    `DISTANCE` ranks rows by smallest value of `ranking_expression` first. `SIMILARITY` ranks rows by largest value first. Matched right-side rows are emitted in best-first order: smallest ranking value first under `DISTANCE`, largest first under `SIMILARITY`. (Downstream operators may reorder; add an explicit `ORDER BY` if you need to lock in the ordering.)

    `ranking_expression`

    A scalar expression that returns an orderable type. Must be deterministic with `EXACT`; may be nondeterministic with `APPROX` (e.g., `rand()` for randomized tie-breaking). The expression is evaluated once per (left, right) pair on the brute-force path, so avoid expensive or side-effecting UDFs in ranking expressions.

    **Performance note.** The current implementation evaluates the full cross-product of the left and right sides and bounds memory per left row by `num_results`. Per-query work is `O(|left| × |right| × log num_results)`. Index-backed approximate strategies (transparent to `APPROX` queries) are planned in a future release; until then, pre-filter the right side (e.g. via a subquery) when it is large.

### Join Types

#### **Inner Join**

The inner join is the default join in Spark SQL. It selects rows that have matching values in both relations.

**Syntax:**

`relation [ INNER ] JOIN relation [ join_criteria ]`

#### **Left Join**

A left join returns all values from the left relation and the matched values from the right relation, or appends NULL if there is no match. It is also referred to as a left outer join.

**Syntax:**

`relation LEFT [ OUTER ] JOIN relation [ join_criteria ]`

#### **Right Join**

A right join returns all values from the right relation and the matched values from the left relation, or appends NULL if there is no match. It is also referred to as a right outer join.

**Syntax:**

`relation RIGHT [ OUTER ] JOIN relation [ join_criteria ]`

#### **Full Join**

A full join returns all values from both relations, appending NULL values on the side that does not have a match. It is also referred to as a full outer join.

**Syntax:**

`relation FULL [ OUTER ] JOIN relation [ join_criteria ]`

#### **Cross Join**

A cross join returns the Cartesian product of two relations.

**Syntax:**

`relation CROSS JOIN relation [ join_criteria ]`

#### **Semi Join**

A semi join returns values from the left side of the relation that has a match with the right. It is also referred to as a left semi join.

**Syntax:**

`relation [ LEFT ] SEMI JOIN relation [ join_criteria ]`

#### **Anti Join**

An anti join returns values from the left relation that has no match with the right. It is also referred to as a left anti join.

**Syntax:**

`relation [ LEFT ] ANTI JOIN relation [ join_criteria ]`

### Examples

```sql
-- Use employee and department tables to demonstrate different type of joins.
SELECT * FROM employee;
+---+-----+------+
| id| name|deptno|
+---+-----+------+
|105|Chloe|     5|
|103| Paul|     3|
|101| John|     1|
|102| Lisa|     2|
|104| Evan|     4|
|106|  Amy|     6|
+---+-----+------+

SELECT * FROM department;
+------+-----------+
|deptno|   deptname|
+------+-----------+
|     3|Engineering|
|     2|      Sales|
|     1|  Marketing|
+------+-----------+

-- Use employee and department tables to demonstrate inner join.
SELECT id, name, employee.deptno, deptname
    FROM employee INNER JOIN department ON employee.deptno = department.deptno;
+---+-----+------+-----------|
| id| name|deptno|   deptname|
+---+-----+------+-----------|
|103| Paul|     3|Engineering|
|101| John|     1|  Marketing|
|102| Lisa|     2|      Sales|
+---+-----+------+-----------|

-- Use employee and department tables to demonstrate left join.
SELECT id, name, employee.deptno, deptname
    FROM employee LEFT JOIN department ON employee.deptno = department.deptno;
+---+-----+------+-----------|
| id| name|deptno|   deptname|
+---+-----+------+-----------|
|105|Chloe|     5|       NULL|
|103| Paul|     3|Engineering|
|101| John|     1|  Marketing|
|102| Lisa|     2|      Sales|
|104| Evan|     4|       NULL|
|106|  Amy|     6|       NULL|
+---+-----+------+-----------|

-- Use employee and department tables to demonstrate right join.
SELECT id, name, employee.deptno, deptname
    FROM employee RIGHT JOIN department ON employee.deptno = department.deptno;
+---+-----+------+-----------|
| id| name|deptno|   deptname|
+---+-----+------+-----------|
|103| Paul|     3|Engineering|
|101| John|     1|  Marketing|
|102| Lisa|     2|      Sales|
+---+-----+------+-----------|

-- Use employee and department tables to demonstrate full join.
SELECT id, name, employee.deptno, deptname
    FROM employee FULL JOIN department ON employee.deptno = department.deptno;
+---+-----+------+-----------|
| id| name|deptno|   deptname|
+---+-----+------+-----------|
|101| John|     1|  Marketing|
|106|  Amy|     6|       NULL|
|103| Paul|     3|Engineering|
|105|Chloe|     5|       NULL|
|104| Evan|     4|       NULL|
|102| Lisa|     2|      Sales|
+---+-----+------+-----------|

-- Use employee and department tables to demonstrate cross join.
SELECT id, name, employee.deptno, deptname FROM employee CROSS JOIN department;
+---+-----+------+-----------|
| id| name|deptno|   deptname|
+---+-----+------+-----------|
|105|Chloe|     5|Engineering|
|105|Chloe|     5|  Marketing|
|105|Chloe|     5|      Sales|
|103| Paul|     3|Engineering|
|103| Paul|     3|  Marketing|
|103| Paul|     3|      Sales|
|101| John|     1|Engineering|
|101| John|     1|  Marketing|
|101| John|     1|      Sales|
|102| Lisa|     2|Engineering|
|102| Lisa|     2|  Marketing|
|102| Lisa|     2|      Sales|
|104| Evan|     4|Engineering|
|104| Evan|     4|  Marketing|
|104| Evan|     4|      Sales|
|106|  Amy|     4|Engineering|
|106|  Amy|     4|  Marketing|
|106|  Amy|     4|      Sales|
+---+-----+------+-----------|

-- Use employee and department tables to demonstrate semi join.
SELECT * FROM employee SEMI JOIN department ON employee.deptno = department.deptno;
+---+-----+------+
| id| name|deptno|
+---+-----+------+
|103| Paul|     3|
|101| John|     1|
|102| Lisa|     2|
+---+-----+------+

-- Use employee and department tables to demonstrate anti join.
SELECT * FROM employee ANTI JOIN department ON employee.deptno = department.deptno;
+---+-----+------+
| id| name|deptno|
+---+-----+------+
|105|Chloe|     5|
|104| Evan|     4|
|106|  Amy|     6|
+---+-----+------+
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
* [Hints](sql-ref-syntax-qry-select-hints.html)
* [LATERAL Subquery](sql-ref-syntax-qry-select-lateral-subquery.html)
