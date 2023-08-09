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
relation { [ join_type ] JOIN [ LATERAL ] relation [ join_criteria ] | NATURAL join_type JOIN [ LATERAL ] relation }
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
