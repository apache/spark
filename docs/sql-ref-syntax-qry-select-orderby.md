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

### Description

The `ORDER BY` clause is used to return the result rows in a sorted manner
in the user specified order. Unlike the [SORT BY](sql-ref-syntax-qry-select-sortby.html)
clause, this clause guarantees a total order in the output.

### Syntax

```sql
ORDER BY { expression [ sort_direction | nulls_sort_order ] [ , ... ] }
```

### Parameters

* **ORDER BY**

    Specifies a comma-separated list of expressions along with optional parameters `sort_direction`
    and `nulls_sort_order` which are used to sort the rows.

* **sort_direction**

    Optionally specifies whether to sort the rows in ascending or descending
    order. The valid values for the sort direction are `ASC` for ascending
    and `DESC` for descending. If sort direction is not explicitly specified, then by default
    rows are sorted ascending.

    **Syntax:** [ ASC `|` DESC ]

* **nulls_sort_order**

    Optionally specifies whether NULL values are returned before/after non-NULL values. If
    `null_sort_order` is not specified, then NULLs sort first if sort order is
    `ASC` and NULLS sort last if sort order is `DESC`.

    1. If `NULLS FIRST` is specified, then NULL values are returned first
       regardless of the sort order.
    2. If `NULLS LAST` is specified, then NULL values are returned last regardless of
       the sort order.

    **Syntax:** `[ NULLS { FIRST | LAST } ]`

### Examples

```sql
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
| name| age|
+-----+----+
|Jerry|null|
| Mary|null|
| John|  30|
|  Dan|  50|
| Mike|  80|
+-----+----+

-- Sort rows in ascending manner keeping null values to be last.
SELECT name, age FROM person ORDER BY age NULLS LAST;
+-----+----+
| name| age|
+-----+----+
| John|  30|
|  Dan|  50|
| Mike|  80|
| Mary|null|
|Jerry|null|
+-----+----+

-- Sort rows by age in descending manner, which defaults to NULL LAST.
SELECT name, age FROM person ORDER BY age DESC;
+-----+----+
| name| age|
+-----+----+
| Mike|  80|
|  Dan|  50|
| John|  30|
|Jerry|null|
| Mary|null|
+-----+----+

-- Sort rows in ascending manner keeping null values to be first.
SELECT name, age FROM person ORDER BY age DESC NULLS FIRST;
+-----+----+
| name| age|
+-----+----+
|Jerry|null|
| Mary|null|
| Mike|  80|
|  Dan|  50|
| John|  30|
+-----+----+

-- Sort rows based on more than one column with each column having different
-- sort direction.
SELECT * FROM person ORDER BY name ASC, age DESC;
+---+-----+----+
| id| name| age|
+---+-----+----+
|500|  Dan|  50|
|400|Jerry|null|
|100| John|  30|
|200| Mary|null|
|300| Mike|  80|
+---+-----+----+
```

### Related Statements

* [SELECT Main](sql-ref-syntax-qry-select.html)
* [WHERE Clause](sql-ref-syntax-qry-select-where.html)
* [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
* [HAVING Clause](sql-ref-syntax-qry-select-having.html)
* [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
* [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
* [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
* [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
* [CASE Clause](sql-ref-syntax-qry-select-case.html)
* [PIVOT Clause](sql-ref-syntax-qry-select-pivot.html)
* [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
