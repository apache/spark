---
layout: global
title: QUALIFY Clause
displayTitle: QUALIFY Clause
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

The `QUALIFY` clause filters rows after window functions have been evaluated.
It can refer to window functions in the `SELECT` list by alias, or define window
functions directly in the `QUALIFY` condition.

### Syntax

```sql
QUALIFY boolean_expression
```

### Parameters

* **boolean_expression**

    Specifies any expression that evaluates to a result type `boolean`. Two or
    more expressions may be combined together using the logical
    operators ( `AND`, `OR` ).

    **Note**

    The current query's `SELECT` list or the `QUALIFY` condition must contain at least
    one window function. Aggregate functions are not allowed in the `QUALIFY` condition.

### Examples

```sql
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

-- `QUALIFY` clause referring to a window function in the `SELECT` list by alias.
SELECT city, car_model, RANK() OVER (PARTITION BY car_model ORDER BY quantity) AS rank
FROM dealer
QUALIFY rank = 1;
+--------+------------+----+
|    city|   car_model|rank|
+--------+------------+----+
|San Jose|Honda Accord|   1|
|  Dublin|   Honda CRV|   1|
|San Jose| Honda Civic|   1|
+--------+------------+----+

-- `QUALIFY` clause with a window function directly in the predicate.
SELECT city, car_model
FROM dealer
QUALIFY RANK() OVER (PARTITION BY car_model ORDER BY quantity) = 1;
+--------+------------+
|    city|   car_model|
+--------+------------+
|San Jose|Honda Accord|
|  Dublin|   Honda CRV|
|San Jose| Honda Civic|
+--------+------------+
```

### Related Statements

* [SELECT Main](sql-ref-syntax-qry-select.html)
* [WHERE Clause](sql-ref-syntax-qry-select-where.html)
* [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
* [HAVING Clause](sql-ref-syntax-qry-select-having.html)
* [WINDOW Clause](sql-ref-syntax-qry-select-window.html)
* [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
* [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
* [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
* [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
* [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
* [OFFSET Clause](sql-ref-syntax-qry-select-offset.html)
