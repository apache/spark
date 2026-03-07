---
layout: global
title: Temporal Clause
displayTitle: Temporal Clause
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

The temporal clause enables time travel when querying a table. It can be specified after a table name in the FROM clause. Time travel allows you to query a table at a specific version or point in time, which is useful for auditing, reproducing experiments, or recovering from accidental changes. Time travel is supported by data sources that implement this capability, such as Delta Lake.

### Syntax

```sql
table_name [ FOR ] ( SYSTEM_VERSION | VERSION ) AS OF version
table_name [ FOR ] ( SYSTEM_TIME | TIMESTAMP ) AS OF timestamp_expression
```

### Parameters

* **VERSION** or **SYSTEM_VERSION**

    Specifies the version to query. `VERSION` and `SYSTEM_VERSION` are synonymous; both produce the same result. The version can be specified as an integer (e.g., `123456789`) or a string literal (e.g., `'Snapshot123456789'`).

* **TIMESTAMP** or **SYSTEM_TIME**

    Specifies the point in time to query. `TIMESTAMP` and `SYSTEM_TIME` are synonymous; both produce the same result. The timestamp can be:
    * A literal string (e.g., `'2019-01-29 00:37:58'`)
    * An expression that evaluates to a timestamp (e.g., `current_date()`, `current_timestamp()`)
    * A subquery that returns a single timestamp value (e.g., `(SELECT max(timestamp_column) FROM history_table)`)

    The timestamp expression cannot refer to any columns from the query.

* **FOR**

    Optional keyword. Can be specified before `VERSION`/`SYSTEM_VERSION` or `TIMESTAMP`/`SYSTEM_TIME` for SQL standard compatibility.

### Limitations

* Time travel is supported only for tables that implement this capability (e.g., Delta Lake). Views and subqueries do not support time travel.
* The table must be a v2 data source.
* When using `TIMESTAMP AS OF`, the timestamp expression cannot refer to any columns; it must be a constant or a deterministic expression evaluated at query planning time.

### Examples

**Query by version:**

```sql
-- Using VERSION (or SYSTEM_VERSION)
SELECT * FROM person VERSION AS OF 123;
SELECT * FROM person FOR VERSION AS OF 123;

-- Version can also be a string
SELECT * FROM person VERSION AS OF 'Snapshot123456789';
```

**Query by timestamp:**

```sql
-- Using TIMESTAMP (or SYSTEM_TIME)
SELECT * FROM person TIMESTAMP AS OF '2019-01-29 00:37:58';
SELECT * FROM person FOR TIMESTAMP AS OF '2019-01-29 00:37:58';

-- Using a timestamp expression
SELECT * FROM person TIMESTAMP AS OF current_timestamp() - INTERVAL 1 DAY;
SELECT * FROM person TIMESTAMP AS OF date_sub(current_date(), 1);
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
* [UNPIVOT Clause](sql-ref-syntax-qry-select-unpivot.html)
* [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
