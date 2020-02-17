---
layout: global
title: SORT BY Clause
displayTitle: SORT BY Clause
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
The <code>SORT BY</code> clause is used to return the result rows sorted
within each partition in the user specified order. When there is more than one partition
<code>SORT BY</code> may return result that is partially ordered. This is different
than [ORDER BY](sql-ref-syntax-qry-select-orderby.html) clause which guarantees a
total order of the output.

### Syntax
{% highlight sql %}
SORT BY { expression [ sort_direction | nulls_sort_order ] [ , ... ] }
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>SORT BY</em></code></dt>
  <dd>
    Specifies a comma-separated list of expressions along with optional parameters <code>sort_direction</code>
    and <code>nulls_sort_order</code> which are used to sort the rows within each partition.
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
    Optionally specifies whether NULL values are returned before/after non-NULL values, based on the 
    sort direction. In Spark, NULL values are considered to be lower than any non-NULL values by default.
    Therefore the ordering of NULL values depend on the sort direction. If <code>null_sort_order</code> is
    not specified, then NULLs sort first if sort order is <code>ASC</code> and NULLS sort last if 
    sort order is <code>DESC</code>.<br><br>
    <ol>
      <li> If <code>NULLS FIRST</code> (the default) is specified, then NULL values are returned first 
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
CREATE TABLE person (zip_code INT, name STRING, age INT);
INSERT INTO person VALUES
    (94588, 'Zen Hui', 50), 
    (94588, 'Dan Li', 18), 
    (94588, 'Anil K', 27),
    (94588, 'John V', NULL),
    (94511, 'David K', 42),
    (94511, 'Aryan B.', 18),
    (94511, 'Lalit B.', NULL);

-- Use `REPARTITION` hint to partition the data by `zip_code` to 
-- examine the `SORT BY` behavior. This is used in rest of the
-- examples.

-- Sort rows by `name` within each partition in ascending manner
SELECT /*+ REPARTITION(zip_code) */ name, age, zip_code FROM person SORT BY name;

  +--------+----+--------+
  |name    |age |zip_code|
  +--------+----+--------+
  |Anil K  |27  |94588   |
  |Dan Li  |18  |94588   |
  |John V  |null|94588   |
  |Zen Hui |50  |94588   |
  |Aryan B.|18  |94511   |
  |David K |42  |94511   |
  |Lalit B.|null|94511   |
  +--------+----+--------+

-- Sort rows within each partition using column position.
SELECT /*+ REPARTITION(zip_code) */ name, age, zip_code FROM person SORT BY 1;

  +--------+----+--------+
  |name    |age |zip_code|
  +--------+----+--------+
  |Anil K  |27  |94588   |
  |Dan Li  |18  |94588   |
  |John V  |null|94588   |
  |Zen Hui |50  |94588   |
  |Aryan B.|18  |94511   |
  |David K |42  |94511   |
  |Lalit B.|null|94511   |
  +--------+----+--------+

-- Sort rows within partition in ascending manner keeping null values to be last.
SELECT /*+ REPARTITION(zip_code) */ age, name, zip_code FROM person SORT BY age NULLS LAST;

  +----+--------+--------+
  |age |name    |zip_code|
  +----+--------+--------+
  |18  |Dan Li  |94588   |
  |27  |Anil K  |94588   |
  |50  |Zen Hui |94588   |
  |null|John V  |94588   |
  |18  |Aryan B.|94511   |
  |42  |David K |94511   |
  |null|Lalit B.|94511   |
  +----+--------+--------+

-- Sort rows by age within each partition in descending manner.
SELECT /*+ REPARTITION(zip_code) */ age, name, zip_code FROM person SORT BY age DESC;
 
  +----+--------+--------+
  |age |name    |zip_code|
  +----+--------+--------+
  |50  |Zen Hui |94588   |
  |27  |Anil K  |94588   |
  |18  |Dan Li  |94588   |
  |null|John V  |94588   |
  |42  |David K |94511   |
  |18  |Aryan B.|94511   |
  |null|Lalit B.|94511   |
  +----+--------+--------+

-- Sort rows by age within each partition in ascending manner keeping null values to be first.
SELECT /*+ REPARTITION(zip_code) */ age, name, zip_code FROM person SORT BY age DESC NULLS FIRST;

  +----+--------+--------+
  |age |name    |zip_code|
  +----+--------+--------+
  |null|John V  |94588   |
  |50  |Zen Hui |94588   |
  |27  |Anil K  |94588   |
  |18  |Dan Li  |94588   |
  |null|Lalit B.|94511   |
  |42  |David K |94511   |
  |18  |Aryan B.|94511   |
  +----+--------+--------+

-- Sort rows within each partition based on more than one column with each column having
-- different sort direction.
SELECT /*+ REPARTITION(zip_code) */ name, age, zip_code FROM person
   SORT BY name ASC, age DESC;

  +--------+----+--------+
  |name    |age |zip_code|
  +--------+----+--------+
  |Anil K  |27  |94588   |
  |Dan Li  |18  |94588   |
  |John V  |null|94588   |
  |Zen Hui |50  |94588   |
  |Aryan B.|18  |94511   |
  |David K |42  |94511   |
  |Lalit B.|null|94511   |
  +--------+----+--------+
{% endhighlight %}

### Related Clauses
- [SELECT Main](sql-ref-syntax-qry-select.html)
- [WHERE Clause](sql-ref-syntax-qry-select-where.html)
- [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
- [HAVING Clause](sql-ref-syntax-qry-select-having.html)
- [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
- [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
- [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
- [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
