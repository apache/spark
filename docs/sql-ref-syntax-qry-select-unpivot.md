---
layout: global
title: UNPIVOT Clause
displayTitle: UNPIVOT Clause
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

The `UNPIVOT` clause transforms multiple columns into multiple rows used in `SELECT` clause.
The `UNPIVOT` clause can be specified after the table name or subquery.

### Syntax

```sql
UNPIVOT [ { INCLUDE | EXCLUDE } NULLS ] (
    { single_value_column_unpivot | multi_value_column_unpivot }
) [[AS] alias]

single_value_column_unpivot:
    values_column
    FOR name_column
    IN (unpivot_column [[AS] alias] [, ...])

multi_value_column_unpivot:
    (values_column [, ...])
    FOR name_column
    IN ((unpivot_column [, ...]) [[AS] alias] [, ...])
```

### Parameters

* **unpivot_column**

    Contains columns in the `FROM` clause, which specifies the columns we want to unpivot.

* **name_column**

    The name for the column that holds the names of the unpivoted columns.

* **values_column**

    The name for the column that holds the values of the unpivoted columns.

### Examples

```sql
CREATE TABLE sales_quarterly (year INT, q1 INT, q2 INT, q3 INT, q4 INT);
INSERT INTO sales_quarterly VALUES
    (2020, null, 1000, 2000, 2500),
    (2021, 2250, 3200, 4200, 5900),
    (2022, 4200, 3100, null, null);

-- column names are used as unpivot columns
SELECT * FROM sales_quarterly
    UNPIVOT (
        sales FOR quarter IN (q1, q2, q3, q4)
    );
+------+---------+-------+
| year | quarter | sales |
+------+---------+-------+
| 2020 | q2      | 1000  |
| 2020 | q3      | 2000  |
| 2020 | q4      | 2500  |
| 2021 | q1      | 2250  |
| 2021 | q2      | 3200  |
| 2021 | q3      | 4200  |
| 2021 | q4      | 5900  |
| 2022 | q1      | 4200  |
| 2022 | q2      | 3100  |
+------+---------+-------+

-- NULL values are excluded by default, they can be included
-- unpivot columns can be alias
-- unpivot result can be referenced via its alias
SELECT up.* FROM sales_quarterly
    UNPIVOT INCLUDE NULLS (
        sales FOR quarter IN (q1 AS Q1, q2 AS Q2, q3 AS Q3, q4 AS Q4)
    ) AS up;
+------+---------+-------+
| year | quarter | sales |
+------+---------+-------+
| 2020 | Q1      | NULL  |
| 2020 | Q2      | 1000  |
| 2020 | Q3      | 2000  |
| 2020 | Q4      | 2500  |
| 2021 | Q1      | 2250  |
| 2021 | Q2      | 3200  |
| 2021 | Q3      | 4200  |
| 2021 | Q4      | 5900  |
| 2022 | Q1      | 4200  |
| 2022 | Q2      | 3100  |
| 2022 | Q3      | NULL  |
| 2022 | Q4      | NULL  |
+------+---------+-------+

-- multiple value columns can be unpivoted per row
SELECT * FROM sales_quarterly
    UNPIVOT EXCLUDE NULLS (
        (first_quarter, second_quarter)
        FOR half_of_the_year IN (
            (q1, q2) AS H1,
            (q3, q4) AS H2
        )
    );
+------+------------------+---------------+----------------+
|  id  | half_of_the_year | first_quarter | second_quarter |
+------+------------------+---------------+----------------+
| 2020 | H1               | NULL          | 1000           |
| 2020 | H2               | 2000          | 2500           |
| 2021 | H1               | 2250          | 3200           |
| 2021 | H2               | 4200          | 5900           |
| 2022 | H1               | 4200          | 3100           |
+------+------------------+---------------+----------------+
```

### Related Statements

* [SELECT Main](sql-ref-syntax-qry-select.html)
* [WHERE Clause](sql-ref-syntax-qry-select-where.html)
* [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
* [HAVING Clause](sql-ref-syntax-qry-select-having.html)
* [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
* [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
* [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
* [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
* [OFFSET Clause](sql-ref-syntax-qry-select-offset.html)
* [CASE Clause](sql-ref-syntax-qry-select-case.html)
* [PIVOT Clause](sql-ref-syntax-qry-select-pivot.html)
* [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
