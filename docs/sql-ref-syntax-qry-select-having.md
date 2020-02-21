---
layout: global
title: HAVING Clause
displayTitle: HAVING Clause
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
The <code>HAVING</code> clause is used to filter the results produced by
<code>GROUP BY</code> based on the specified condition. It is often used
in conjunction with a [GROUP BY](sql-ref-syntax-qry-select-groupby.html)
clause.

### Syntax
{% highlight sql %}
HAVING boolean_expression
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>boolean_expression</em></code></dt>
  <dd>
    Specifies any expression that evaluates to a result type <code>boolean</code>. Two or
    more expressions may be combined together using the logical 
    operators ( <code>AND</code>, <code>OR</code> ).<br><br>

    <b>Note</b><br>
    The expressions specified in the <code>HAVING</code> clause can only refer to:
     <ol>
      <li>Constants</li>
      <li>Expressions that appear in GROUP BY</li>
      <li>Aggregate functions</li>
    </ol>
  </dd>
</dl>

### Examples
{% highlight sql %}
CREATE TABLE dealer (id INT, city STRING, car_model STRING, quantity INT);
INSERT INTO dealer VALUES
    (100, 'Fremont', 'Honda Civic', 10),
    (100, 'Fremont', 'Honda Accord', 15),
    (100, 'Fremont', 'Honda CRV', 7),
    (200, 'Dublin', 'Honda Civic', 20),
    (200, 'Dublin', 'Honda Accord', 10),
    (200, 'Dublin', 'Honda CRV', 3),
    (300, 'San Jose', 'Honda Civic', 5),
    (300, 'San Jose', 'Honda Accord', 8);

-- `HAVING` clause referring to column in `GROUP BY`.
SELECT city, sum(quantity) AS sum FROM dealer GROUP BY city HAVING city = 'Fremont';

  +-------+---+
  |city   |sum|
  +-------+---+
  |Fremont|32 |
  +-------+---+

-- `HAVING` clause referring to aggregate function.
SELECT city, sum(quantity) AS sum FROM dealer GROUP BY city HAVING sum(quantity) > 15;
 
  +-------+---+
  |   city|sum|
  +-------+---+
  | Dublin| 33|
  |Fremont| 32|
  +-------+---+

-- `HAVING` clause referring to aggregate function by its alias.
SELECT city, sum(quantity) AS sum FROM dealer GROUP BY city HAVING sum > 15;

  +-------+---+
  |   city|sum|
  +-------+---+
  | Dublin| 33|
  |Fremont| 32|
  +-------+---+

-- `HAVING` clause referring to a different aggregate function than what is present in
-- `SELECT` list.
SELECT city, sum(quantity) AS sum FROM dealer GROUP BY city HAVING max(quantity) > 15;

  +------+---+
  |city  |sum|
  +------+---+
  |Dublin|33 |
  +------+---+

-- `HAVING` clause referring to constant expression.
SELECT city, sum(quantity) AS sum FROM dealer GROUP BY city HAVING 1 > 0 ORDER BY city;
  
  +--------+---+
  |    city|sum|
  +--------+---+
  |  Dublin| 33|
  | Fremont| 32|
  |San Jose| 13|
  +--------+---+

-- `HAVING` clause without a `GROUP BY` clause.
SELECT sum(quantity) AS sum FROM dealer HAVING sum(quantity) > 10;
  +---+
  |sum|
  +---+
  | 78|
  +---+
 
{% endhighlight %}

### Related Clauses
- [SELECT Main](sql-ref-syntax-qry-select.html)
- [WHERE Clause](sql-ref-syntax-qry-select-where.html)
- [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
- [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
- [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
- [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
- [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
- [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
