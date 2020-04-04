---
layout: global
title: CROSS JOIN
displayTitle: CROSS JOIN
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

A cross join returns the Cartesian product of two relations.

### Syntax
{% highlight sql %}

relation CROSS JOIN relation [ join_criteria ]

{% endhighlight %}

### Examples
{% highlight sql %}
-- Use employee and department tables to demonstrate cross join.

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

SELECT id, name, employee.deptno, deptname FROM employee CROSS JOIN department;
  +---+-----+-------+-----------|
  |id |name |deptno |deptname   |
  +---+-----+-------+-----------|
  |105|Chloe|5      |Engineering|
  |105|Chloe|5      |Marketing  |
  |105|Chloe|5      |Sales      |
  |103|Paul |3      |Engineering|
  |103|Paul |3      |Marketing  |
  |103|Paul |3      |Sales      |
  |101|John |1      |Engineering|
  |101|John |1      |Marketing  |
  |101|John |1      |Sales      |
  |102|Lisa |2      |Engineering|
  |102|Lisa |2      |Marketing  |
  |102|Lisa |2      |Sales      |
  |104|Evan |4      |Engineering|
  |104|Evan |4      |Marketing  |
  |104|Evan |4      |Sales      |
  |106|Amy  |4      |Engineering|
  |106|Amy  |4      |Marketing  |
  |106|Amy  |4      |Sales      |
  +---+-----+-------+-----------|

{% endhighlight %}

### Related Statements
- [JOIN](sql-ref-syntax-qry-select-join.html)
- [INNER JOIN](sql-ref-syntax-qry-select-join-inner.html)
- [LEFT JOIN](sql-ref-syntax-qry-select-join-left.html)
- [RIGHT JOIN](sql-ref-syntax-qry-select-join-right.html)
- [FULL JOIN](sql-ref-syntax-qry-select-join-full.html)
- [SEMI JOIN](sql-ref-syntax-qry-select-join-semi.html)
- [ANTI JOIN](sql-ref-syntax-qry-select-join-anti.html)