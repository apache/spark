---
layout: global
title: ORDER BY Clause
displayTitle: ORDER BY Clause
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
The <code>ORDER BY</code> clause is used to return the result rows in a sorted manner
in the user specified order. Unlike the [SORT BY](sql-ref-syntax-qry-select-sortby.html)
clause, this clause guarantees a total order in the output.

### Syntax
{% highlight sql %}
ORDER BY { expression [ sort_direction | nulls_sort_oder ] [ , ... ] }
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>ORDER BY</em></code></dt>
  <dd>
    Specifies a comma-separated list of expressions along with optional parameters <code>sort_direction</code>
    and <code>nulls_sort_order</code> which are used to sort the rows.
  </dd>
  <dt><code><em>sort_direction</em></code></dt>
  <dd>
    Optionally specifies whether to sort the rows in ascending or descending
    order. The valid values for the sort direction are <code>ASC</code> for ascending
    and <code>DESC</code> for descending. If sort direction is not explicitly specified, then by default
    rows are sorted ascending. <br><br>
    <b>Syntax:</b>
    <code>
       [ ASC | DESC ]
    </code>
  </dd>
  <dt><code><em>nulls_sort_order</em></code></dt>
  <dd>
    Optionally specifies whether NULL values are returned before/after non-NULL values. If
    <code>null_sort_order</code> is not specified, then NULLs sort first if sort order is
    <code>ASC</code> and NULLS sort last if sort order is <code>DESC</code>.<br><br>
    <ol>
      <li> If <code>NULLS FIRST</code> is specified, then NULL values are returned first
           regardless of the sort order.</li>
      <li>If <code>NULLS LAST</code> is specified, then NULL values are returned last regardless of
           the sort order. </li>
    </ol><br>
    <b>Syntax:</b>
    <code>
       [ NULLS { FIRST | LAST } ]
    </code>
  </dd>
</dl>

### Examples
{% highlight sql %}
CREATE TABLE person (id INT, name STRING, age INT);
INSERT INTO person VALUES
    (100, 'John', 30),
    (200, 'Mary', NULL),
    (300, 'Mike', 80),
    (400, 'Jerry', NULL),
    (500, 'Dan',  50);

-- Sort rows by age. By default rows are sorted in ascending manner with NULL FIRST.
SELECT name, age FROM person ORDER BY age;

  +-----+----+
  |name |age |
  +-----+----+
  |Jerry|null|
  |Mary |null|
  |John |30  |
  |Dan  |50  |
  |Mike |80  |
  +-----+----+

-- Sort rows in ascending manner keeping null values to be last.
SELECT name, age FROM person ORDER BY age NULLS LAST;

  +-----+----+
  |name |age |
  +-----+----+
  |John |30  |
  |Dan  |50  |
  |Mike |80  |
  |Mary |null|
  |Jerry|null|
  +-----+----+

-- Sort rows by age in descending manner, which defaults to NULL LAST.
SELECT name, age FROM person ORDER BY age DESC;

  +-----+----+
  |name |age |
  +-----+----+
  |Mike |80  |
  |Dan  |50  |
  |John |30  |
  |Jerry|null|
  |Mary |null|
  +-----+----+

-- Sort rows in ascending manner keeping null values to be first.
SELECT name, age FROM person ORDER BY age DESC NULLS FIRST;

  +-----+----+
  |name |age |
  +-----+----+
  |Jerry|null|
  |Mary |null|
  |Mike |80  |
  |Dan  |50  |
  |John |30  |
  +-----+----+

-- Sort rows based on more than one column with each column having different
-- sort direction.
SELECT * FROM person ORDER BY name ASC, age DESC;

  +---+-----+----+
  |id |name |age |
  +---+-----+----+
  |500|Dan  |50  |
  |400|Jerry|null|
  |100|John |30  |
  |200|Mary |null|
  |300|Mike |80  |
  +---+-----+----+
{% endhighlight %}

### Related Clauses
- [SELECT Main](sql-ref-syntax-qry-select.html)
- [WHERE Clause](sql-ref-syntax-qry-select-where.html)
- [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
- [HAVING Clause](sql-ref-syntax-qry-select-having.html)
- [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
- [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
- [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
- [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
