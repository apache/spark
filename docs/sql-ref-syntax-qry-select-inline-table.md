---
layout: global
title: Inline Table
displayTitle: Inline Table
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

An inline table is a temporary table created using a VALUES clause.

### Syntax

```sql
VALUES ( expression [ , ... ] ) [ table_alias ]
```

### Parameters

* **expression**

    Specifies a combination of one or more values, operators and SQL functions that results in a value.
    Since Spark 4.4, the expression does not need to be constant: nondeterministic functions such as
    `rand()`, Scala and Java user-defined functions, and uncorrelated scalar subqueries are evaluated
    when the query runs. Aggregate, window and generator functions, SQL and Python user-defined
    functions, other kinds of subqueries, and references to columns of an outer query are not
    supported.

* **table_alias**

    Specifies a temporary name with an optional column name list.

    **Syntax:** `[ AS ] table_name [ ( column_name [ , ... ] ) ]`

### Examples

```sql
-- single row, without a table alias
SELECT * FROM VALUES ("one", 1);
+----+----+
|col1|col2|
+----+----+
| one|   1|
+----+----+

-- three rows with a table alias
SELECT * FROM VALUES ("one", 1), ("two", 2), ("three", null) AS data(a, b);
+-----+----+
|    a|   b|
+-----+----+
|  one|   1|
|  two|   2|
|three|null|
+-----+----+

-- complex types with a table alias
SELECT * FROM VALUES ("one", array(0, 1)), ("two", array(2, 3)) AS data(a, b);
+---+------+
|  a|     b|
+---+------+
|one|[0, 1]|
|two|[2, 3]|
+---+------+

-- expressions that are not constant, such as nondeterministic functions and scalar subqueries
SELECT a, length(b) FROM VALUES (1, uuid()), ((SELECT max(id) FROM range(5)), uuid()) AS data(a, b);
+---+---------+
|  a|length(b)|
+---+---------+
|  1|       36|
|  4|       36|
+---+---------+
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
