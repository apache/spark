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

### Description

The `GROUP BY` clause is used to group the rows based on a set of specified grouping expressions and compute aggregations on
the group of rows based on one or more specified aggregate functions. Spark also supports advanced aggregations to do multiple
aggregations for the same input record set via `GROUPING SETS`, `CUBE`, `ROLLUP` clauses.
The grouping expressions and advanced aggregations can be mixed in the `GROUP BY` clause and nested in a `GROUPING SETS` clause.
See more details in the `Mixed/Nested Grouping Analytics` section. When a FILTER clause is attached to
an aggregate function, only the matching rows are passed to that function.

### Syntax

```sql
GROUP BY group_expression [ , group_expression [ , ... ] ] [ WITH { ROLLUP | CUBE } ]

GROUP BY { group_expression | { ROLLUP | CUBE | GROUPING SETS } (grouping_set [ , ...]) } [ , ... ]
```

While aggregate functions are defined as
```sql
aggregate_name ( [ DISTINCT ] expression [ , ... ] ) [ FILTER ( WHERE boolean_expression ) ]
```

### Parameters

* **group_expression**

    Specifies the criteria based on which the rows are grouped together. The grouping of rows is performed based on
    result values of the grouping expressions. A grouping expression may be a column name like `GROUP BY a`, a column position like
    `GROUP BY 0`, or an expression like `GROUP BY a + b`.

* **grouping_set**

    A grouping set is specified by zero or more comma-separated expressions in parentheses. When the
    grouping set has only one element, parentheses can be omitted. For example, `GROUPING SETS ((a), (b))`
    is the same as `GROUPING SETS (a, b)`.

    **Syntax:** `{ ( [ expression [ , ... ] ] ) | expression }`

* **GROUPING SETS**

    Groups the rows for each grouping set specified after GROUPING SETS. For example,
    `GROUP BY GROUPING SETS ((warehouse), (product))` is semantically equivalent
    to union of results of `GROUP BY warehouse` and `GROUP BY product`. This clause
    is a shorthand for a `UNION ALL` where each leg of the `UNION ALL`
    operator performs aggregation of each grouping set specified in the `GROUPING SETS` clause.
    Similarly, `GROUP BY GROUPING SETS ((warehouse, product), (product), ())` is semantically
    equivalent to the union of results of `GROUP BY warehouse, product`, `GROUP BY product`
    and global aggregate.
    
    **Note:** For Hive compatibility Spark allows `GROUP BY ... GROUPING SETS (...)`. The GROUP BY
    expressions are usually ignored, but if it contains extra expressions than the GROUPING SETS
    expressions, the extra expressions will be included in the grouping expressions and the value
    is always null. For example, `SELECT a, b, c FROM ... GROUP BY a, b, c GROUPING SETS (a, b)`,
    the output of column `c` is always null.

* **ROLLUP**

    Specifies multiple levels of aggregations in a single statement. This clause is used to compute aggregations
    based on multiple grouping sets. `ROLLUP` is a shorthand for `GROUPING SETS`. For example,
    `GROUP BY warehouse, product WITH ROLLUP` or `GROUP BY ROLLUP(warehouse, product)` is equivalent to
    `GROUP BY GROUPING SETS((warehouse, product), (warehouse), ())`.
    `GROUP BY ROLLUP(warehouse, product, (warehouse, location))` is equivalent to
    `GROUP BY GROUPING SETS((warehouse, product, location), (warehouse, product), (warehouse), ())`.
    The N elements of a `ROLLUP` specification results in N+1 `GROUPING SETS`.

* **CUBE**

    `CUBE` clause is used to perform aggregations based on combination of grouping columns specified in the
    `GROUP BY` clause. `CUBE` is a shorthand for `GROUPING SETS`. For example,
    `GROUP BY warehouse, product WITH CUBE` or `GROUP BY CUBE(warehouse, product)` is equivalent to 
    `GROUP BY GROUPING SETS((warehouse, product), (warehouse), (product), ())`.
    `GROUP BY CUBE(warehouse, product, (warehouse, location))` is equivalent to
    `GROUP BY GROUPING SETS((warehouse, product, location), (warehouse, product), (warehouse, location),
     (product, warehouse, location), (warehouse), (product), (warehouse, product), ())`.
    The N elements of a `CUBE` specification results in 2^N `GROUPING SETS`.

* **Mixed/Nested Grouping Analytics**

    A GROUP BY clause can include multiple `group_expression`s and multiple `CUBE|ROLLUP|GROUPING SETS`s.
    `GROUPING SETS` can also have nested `CUBE|ROLLUP|GROUPING SETS` clauses, e.g.
    `GROUPING SETS(ROLLUP(warehouse, location), CUBE(warehouse, location))`,
    `GROUPING SETS(warehouse, GROUPING SETS(location, GROUPING SETS(ROLLUP(warehouse, location), CUBE(warehouse, location))))`.
    `CUBE|ROLLUP` is just a syntax sugar for `GROUPING SETS`, please refer to the sections above for
    how to translate `CUBE|ROLLUP` to `GROUPING SETS`. `group_expression` can be treated as a single-group
    `GROUPING SETS` under this context. For multiple `GROUPING SETS` in the `GROUP BY` clause, we generate
    a single `GROUPING SETS` by doing a cross-product of the original `GROUPING SETS`s. For nested `GROUPING SETS` in the `GROUPING SETS` clause,
    we simply take its grouping sets and strip it. For example,
    `GROUP BY warehouse, GROUPING SETS((product), ()), GROUPING SETS((location, size), (location), (size), ())`
    and `GROUP BY warehouse, ROLLUP(product), CUBE(location, size)` is equivalent to 
    `GROUP BY GROUPING SETS(
        (warehouse, product, location, size), 
        (warehouse, product, location),
        (warehouse, product, size), 
        (warehouse, product),
        (warehouse, location, size),
        (warehouse, location),
        (warehouse, size),
        (warehouse))`.
    
    `GROUP BY GROUPING SETS(GROUPING SETS(warehouse), GROUPING SETS((warehouse, product)))` is equivalent to 
    `GROUP BY GROUPING SETS((warehouse), (warehouse, product))`.

* **aggregate_name**

    Specifies an aggregate function name (MIN, MAX, COUNT, SUM, AVG, etc.).

* **DISTINCT**

    Removes duplicates in input rows before they are passed to aggregate functions.

* **FILTER**

    Filters the input rows for which the `boolean_expression` in the `WHERE` clause evaluates
    to true are passed to the aggregate function; other rows are discarded.

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

-- Sum of quantity per dealership. Group by `id`.
SELECT id, sum(quantity) FROM dealer GROUP BY id ORDER BY id;
+---+-------------+
| id|sum(quantity)|
+---+-------------+
|100|           32|
|200|           33|
|300|           13|
+---+-------------+

-- Use column position in GROUP by clause.
SELECT id, sum(quantity) FROM dealer GROUP BY 1 ORDER BY 1;
+---+-------------+
| id|sum(quantity)|
+---+-------------+
|100|           32|
|200|           33|
|300|           13|
+---+-------------+

-- Multiple aggregations.
-- 1. Sum of quantity per dealership.
-- 2. Max quantity per dealership.
SELECT id, sum(quantity) AS sum, max(quantity) AS max FROM dealer GROUP BY id ORDER BY id;
+---+---+---+
| id|sum|max|
+---+---+---+
|100| 32| 15|
|200| 33| 20|
|300| 13|  8|
+---+---+---+

-- Count the number of distinct dealer cities per car_model.
SELECT car_model, count(DISTINCT city) AS count FROM dealer GROUP BY car_model;
+------------+-----+
|   car_model|count|
+------------+-----+
| Honda Civic|    3|
|   Honda CRV|    2|
|Honda Accord|    3|
+------------+-----+

-- Sum of only 'Honda Civic' and 'Honda CRV' quantities per dealership.
SELECT id, sum(quantity) FILTER (
            WHERE car_model IN ('Honda Civic', 'Honda CRV')
        ) AS `sum(quantity)` FROM dealer
    GROUP BY id ORDER BY id;
+---+-------------+
| id|sum(quantity)|
+---+-------------+
|100|           17|
|200|           23|
|300|            5|
+---+-------------+

-- Aggregations using multiple sets of grouping columns in a single statement.
-- Following performs aggregations based on four sets of grouping columns.
-- 1. city, car_model
-- 2. city
-- 3. car_model
-- 4. Empty grouping set. Returns quantities for all city and car models.
SELECT city, car_model, sum(quantity) AS sum FROM dealer
    GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
    ORDER BY city;
+---------+------------+---+
|     city|   car_model|sum|
+---------+------------+---+
|     null|        null| 78|
|     null| HondaAccord| 33|
|     null|    HondaCRV| 10|
|     null|  HondaCivic| 35|
|   Dublin|        null| 33|
|   Dublin| HondaAccord| 10|
|   Dublin|    HondaCRV|  3|
|   Dublin|  HondaCivic| 20|
|  Fremont|        null| 32|
|  Fremont| HondaAccord| 15|
|  Fremont|    HondaCRV|  7|
|  Fremont|  HondaCivic| 10|
| San Jose|        null| 13|
| San Jose| HondaAccord|  8|
| San Jose|  HondaCivic|  5|
+---------+------------+---+

-- Group by processing with `ROLLUP` clause.
-- Equivalent GROUP BY GROUPING SETS ((city, car_model), (city), ())
SELECT city, car_model, sum(quantity) AS sum FROM dealer
    GROUP BY city, car_model WITH ROLLUP
    ORDER BY city, car_model;
+---------+------------+---+
|     city|   car_model|sum|
+---------+------------+---+
|     null|        null| 78|
|   Dublin|        null| 33|
|   Dublin| HondaAccord| 10|
|   Dublin|    HondaCRV|  3|
|   Dublin|  HondaCivic| 20|
|  Fremont|        null| 32|
|  Fremont| HondaAccord| 15|
|  Fremont|    HondaCRV|  7|
|  Fremont|  HondaCivic| 10|
| San Jose|        null| 13|
| San Jose| HondaAccord|  8|
| San Jose|  HondaCivic|  5|
+---------+------------+---+

-- Group by processing with `CUBE` clause.
-- Equivalent GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
SELECT city, car_model, sum(quantity) AS sum FROM dealer
    GROUP BY city, car_model WITH CUBE
    ORDER BY city, car_model;
+---------+------------+---+
|     city|   car_model|sum|
+---------+------------+---+
|     null|        null| 78|
|     null| HondaAccord| 33|
|     null|    HondaCRV| 10|
|     null|  HondaCivic| 35|
|   Dublin|        null| 33|
|   Dublin| HondaAccord| 10|
|   Dublin|    HondaCRV|  3|
|   Dublin|  HondaCivic| 20|
|  Fremont|        null| 32|
|  Fremont| HondaAccord| 15|
|  Fremont|    HondaCRV|  7|
|  Fremont|  HondaCivic| 10|
| San Jose|        null| 13|
| San Jose| HondaAccord|  8|
| San Jose|  HondaCivic|  5|
+---------+------------+---+

--Prepare data for ignore nulls example
CREATE TABLE person (id INT, name STRING, age INT);
INSERT INTO person VALUES
    (100, 'Mary', NULL),
    (200, 'John', 30),
    (300, 'Mike', 80),
    (400, 'Dan', 50);

--Select the first row in column age
SELECT FIRST(age) FROM person;
+--------------------+
| first(age, false)  |
+--------------------+
| NULL               |
+--------------------+

--Get the first row in column `age` ignore nulls,last row in column `id` and sum of column `id`.
SELECT FIRST(age IGNORE NULLS), LAST(id), SUM(id) FROM person;
+-------------------+------------------+----------+
| first(age, true)  | last(id, false)  | sum(id)  |
+-------------------+------------------+----------+
| 30                | 400              | 1000     |
+-------------------+------------------+----------+
```

### Related Statements

* [SELECT Main](sql-ref-syntax-qry-select.html)
* [WHERE Clause](sql-ref-syntax-qry-select-where.html)
* [HAVING Clause](sql-ref-syntax-qry-select-having.html)
* [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
* [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
* [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
* [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
* [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
* [OFFSET Clause](sql-ref-syntax-qry-select-offset.html)
* [CASE Clause](sql-ref-syntax-qry-select-case.html)
* [PIVOT Clause](sql-ref-syntax-qry-select-pivot.html)
* [UNPIVOT Clause](sql-ref-syntax-qry-select-unpivot.html)
* [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
