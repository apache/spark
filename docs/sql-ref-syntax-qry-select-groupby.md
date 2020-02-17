---
layout: global
title: GROUP BY Clause
displayTitle: GROUP BY Clause
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
The <code>GROUP BY</code> clause is used to group the rows based on a set of specified grouping expressions and compute aggregations on 
the group of rows based on one or more specified aggregate functions. Spark also supports advanced aggregations to do multiple 
aggregations for the same input record set via `GROUPING SETS`, `CUBE`, `ROLLUP` clauses.

### Syntax
{% highlight sql %}
GROUP BY [ GROUPING SETS grouping_sets ] group_expression [ , group_expression [ , ... ] ]
    [ ( WITH ROLLUP | WITH CUBE | GROUPING SETS grouping_sets ) ) ]
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>GROUPING SETS</em></code></dt>
  <dd>
    Groups the rows for each subset of the expressions specified in the grouping sets. For example, 
    <code>GROUP BY GROUPING SETS (warehouse, product)</code> is semantically equivalent
    to union of results of <code>GROUP BY warehouse</code> and <code>GROUP BY product</code>. This clause
    is shorthand for a <code>UNION ALL</code> where each leg of the <code>UNION ALL</code> 
    operator performs aggregation of subset of the columns specified in the <code>GROUPING SETS</code> clause.
  </dd>
  <dt><code><em>grouping_sets</em></code></dt>
  <dd>
    Specifies one of more groupings based on which the <code>GROUP BY</code> clause performs aggregations. A grouping
    set is specified by a list of comma-separated expressions in parentheses.<br><br>
    <b>Syntax:</b>
      <code>
        ( () | ( expression [ , ... ] ) )
      </code>
  </dd>
  <dt><code><em>grouping_expression</em></code></dt>
  <dd>
    Specifies the critieria based on which the rows are grouped together. The grouping of rows is performed based on
    result values of the grouping expressions. A grouping expression may be a column alias, a column position
    or an expression.
  </dd>
  <dt><code><em>ROLLUP</em></code></dt>
  <dd>
    Specifies multiple levels of aggregations in a single statement. This clause is used to compute aggregations 
    based on multiple grouping sets. <code>ROLLUP</code> is shorthand for <code>GROUPING SETS</code>. For example,
    GROUP BY warehouse, product  WITH ROLLUP is equivalent to GROUP BY <code>warehouse, product</code> GROUPING SETS
    <code> ((warehouse, product), (warehouse), ())</code>.
    The N elements of a <code>ROLLUP</code> specification results in N+1 <code>GROUPING SETS</code>.
  </dd>
  <dt><code><em>CUBE</em></code></dt>
  <dd>
    <code>CUBE</code> clause is used to perform aggregations based on combination of grouping columns specified in the 
    <code>GROUP BY</code> clause. For example, <code>GROUP BY warehouse, product  WITH CUBE</code> is equivalent 
    to GROUP BY <code>warehouse, product</code> GROUPING SETS <code>((warehouse, product), (warehouse), (product), ())</code>.
    The N elements of a <code>CUBE</code> specification results in 2^N <code>GROUPING SETS</code>.
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

-- Sum of quantity per dealership. Group by `id`.
SELECT id, sum(quantity) FROM dealer GROUP BY id ORDER BY id;

  +---+-------------+
  |id |sum(quantity)|
  +---+-------------+
  |100|32           |
  |200|33           |
  |300|13           |
  +---+-------------+

-- Use column position in GROUP by clause.
SELECT id, sum(quantity) FROM dealer GROUP BY 1 ORDER BY 1;

  +---+-------------+
  |id |sum(quantity)|
  +---+-------------+
  |100|32           |
  |200|33           |
  |300|13           |
  +---+-------------+

-- Multiple aggregations.
-- 1. Sum of quantity per dealership.
-- 2. Max quantity per dealership. 
SELECT id, sum(quantity) AS sum, max(quantity) AS max FROM dealer GROUP BY id ORDER BY id;

  +---+---+---+
  |id |sum|max|
  +---+---+---+
  |100|32 |15 |
  |200|33 |20 |
  |300|13 |8  |
  +---+---+---+

-- Aggregations using multiple sets of grouping columns in a single statement.
-- Following performs aggregations based on four sets of grouping columns.
-- 1. city, car_model
-- 2. city
-- 3. car_model
-- 4. Empty grouping set. Returns quantities for all city and car models.
SELECT city, car_model, sum(quantity) AS sum FROM dealer
   GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
   ORDER BY city;

  +--------+------------+---+
  |city    |car_model   |sum|
  +--------+------------+---+
  |null    |null        |78 |
  |null    |Honda Accord|33 |
  |null    |Honda CRV   |10 |
  |null    |Honda Civic |35 |
  |Dublin  |null        |33 |
  |Dublin  |Honda Accord|10 |
  |Dublin  |Honda CRV   |3  |
  |Dublin  |Honda Civic |20 |
  |Fremont |null        |32 |
  |Fremont |Honda Accord|15 |
  |Fremont |Honda CRV   |7  |
  |Fremont |Honda Civic |10 |
  |San Jose|null        |13 |
  |San Jose|Honda Accord|8  |
  |San Jose|Honda Civic |5  |
  +--------+------------+---+

-- Alternate syntax for `GROUPING SETS` in which both `GROUP BY` and `GROUPING SETS` 
-- specifications are present.
SELECT city, car_model, sum(quantity) AS sum FROM dealer
   GROUP BY city, car_model GROUPING SETS ((city, car_model), (city), (car_model), ())
   ORDER BY city, car_model;

  +--------+------------+---+
  |city    |car_model   |sum|
  +--------+------------+---+
  |null    |null        |78 |
  |null    |Honda Accord|33 |
  |null    |Honda CRV   |10 |
  |null    |Honda Civic |35 |
  |Dublin  |null        |33 |
  |Dublin  |Honda Accord|10 |
  |Dublin  |Honda CRV   |3  |
  |Dublin  |Honda Civic |20 |
  |Fremont |null        |32 |
  |Fremont |Honda Accord|15 |
  |Fremont |Honda CRV   |7  |
  |Fremont |Honda Civic |10 |
  |San Jose|null        |13 |
  |San Jose|Honda Accord|8  |
  |San Jose|Honda Civic |5  |
  +--------+------------+---+

-- Group by processing with `ROLLUP` clause.
-- Equivalent GROUP BY GROUPING SETS ((city, car_model), (city), ())
SELECT city, car_model, sum(quantity) AS sum FROM dealer
   GROUP BY city, car_model WITH ROLLUP
   ORDER BY city, car_model;

  +--------+------------+---+
  |city    |car_model   |sum|
  +--------+------------+---+
  |null    |null        |78 |
  |Dublin  |null        |33 |
  |Dublin  |Honda Accord|10 |
  |Dublin  |Honda CRV   |3  |
  |Dublin  |Honda Civic |20 |
  |Fremont |null        |32 |
  |Fremont |Honda Accord|15 |
  |Fremont |Honda CRV   |7  |
  |Fremont |Honda Civic |10 |
  |San Jose|null        |13 |
  |San Jose|Honda Accord|8  |
  |San Jose|Honda Civic |5  |
  +--------+------------+---+

-- Group by processing with `CUBE` clause.
-- Equivalent GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
SELECT city, car_model, sum(quantity) AS sum FROM dealer
   GROUP BY city, car_model WITH CUBE 
   ORDER BY city, car_model;

  +--------+------------+---+
  |city    |car_model   |sum|
  +--------+------------+---+
  |null    |null        |78 |
  |null    |Honda Accord|33 |
  |null    |Honda CRV   |10 |
  |null    |Honda Civic |35 |
  |Dublin  |null        |33 |
  |Dublin  |Honda Accord|10 |
  |Dublin  |Honda CRV   |3  |
  |Dublin  |Honda Civic |20 |
  |Fremont |null        |32 |
  |Fremont |Honda Accord|15 |
  |Fremont |Honda CRV   |7  |
  |Fremont |Honda Civic |10 |
  |San Jose|null        |13 |
  |San Jose|Honda Accord|8  |
  |San Jose|Honda Civic |5  |
  +--------+------------+---+

{% endhighlight %}

### Related clauses
- [SELECT Main](sql-ref-syntax-qry-select.html)
- [WHERE Clause](sql-ref-syntax-qry-select-where.html)
- [HAVING Clause](sql-ref-syntax-qry-select-having.html)
- [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
- [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
- [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
- [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
- [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
