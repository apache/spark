---
layout: global
title: INNER JOIN
displayTitle: INNER JOIN
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

The inner join is the default join in Spark. It selects rows that have matching values in both relations.

### Syntax
{% highlight sql %}

relation [ INNER ] JOIN relation [ join_criteria ]

{% endhighlight %}

### Examples
{% highlight sql %}
-- Use employee and department tables to demonstrate inner join.

SELECT * FROM employee;

  +---+-----+-------+
  |id |name |deptno |
  +---+-----+-------+
  |105|Chloe|5      |
  |103|Paul |3      |
  |101|John |1      |
  |102|Lisa |2      |
  |104|Evan |4      |
  |106|Amy  |6      |
  +---+-----+-------+

SELECT * FROM department;
  +-------+-----------+
  |deptno |deptname   |
  +-------+-----------+
  |3      |Engineering|
  |2      |Sales      |
  |1      |Marketing  |
  +-------+-----------+

SELECT id, name, employee.deptno, deptname
  FROM employee INNER JOIN department ON employee.deptno = department.deptno;
  +---+-----+-------+-----------|
  |id |name |deptno |deptname   |
  +---+-----+-------+-----------|
  |103|Paul |3      |Engineering|
  |101|John |1      |Marketing  |
  |102|Lisa |2      |Sales      |
  +---+-----+-------+-----------|
{% endhighlight %}

### Related Statements
- [JOIN](sql-ref-syntax-qry-select-join.html)
- [LEFT JOIN](sql-ref-syntax-qry-select-join-left.html)
- [RIGHT JOIN](sql-ref-syntax-qry-select-join-right.html)
- [FULL JOIN](sql-ref-syntax-qry-select-join-full.html)
- [CROSS JOIN](sql-ref-syntax-qry-select-join-cross.html)
- [SEMI JOIN](sql-ref-syntax-qry-select-join-semi.html)
- [ANTI JOIN](sql-ref-syntax-qry-select-join-anti.html)
