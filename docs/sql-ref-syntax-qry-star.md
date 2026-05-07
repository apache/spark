---
layout: global
title: star (*) Clause
displayTitle: Star (*) Clause
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

A shorthand to name all the referenceable columns in the FROM clause or a specific table reference's columns or fields in the FROM clause.
The star clause is most frequently used in the SELECT list.
Spark also supports its use in function invocation and certain n-ary operations within the SELECT list and WHERE clause.

### Syntax

```
[ name . ] * [ except_clause ]

except_clause
   EXCEPT ( { column_name | field_name } [, ...] )
```

### Parameters

* **name**

  If present limits the columns or fields to be named to those in the specified referenceable field, column, or table.

* **except_clause**

  Optionally prunes columns or fields from the referenceable set of columns identified in the select_star clause.

  * **column_name**

    A column that is part of the set of columns that you can reference.

  * **field_name**

    A reference to a field in a column of the set of columns that you can reference.
    If you exclude all fields from a STRUCT, the result is an empty STRUCT.
    Each name must reference a column included in the set of columns that you can reference or their fields.
    Otherwise, Spark SQL raises a UNRESOLVED_COLUMN error. If names overlap or are not unique, Spark raises an EXCEPT_OVERLAPPING_COLUMNS error.

### Examples

```sql
-- Return all columns in the FROM clause
SELECT * FROM VALUES(1, 2) AS TA(c1, c2), VALUES('a', 'b') AS TB(ca, cb);
1  2  a  b

-- Return all columns from TA
SELECT TA.* FROM VALUES(1, 2) AS TA(c1, c2), VALUES('a', 'b') AS TB(ca, cb);
1  2

-- Return all columns except TA.c1 and TB.cb
SELECT * EXCEPT (c1, cb)  FROM VALUES(1, 2) AS TA(c1, c2), VALUES('a', 'b') AS TB(ca, cb);
2  a

-- Return all columns, but strip the field x from the struct.
SELECT TA.* EXCEPT (c1.x) FROM VALUES(named_struct('x', x, 'y', 'y'), 2) AS (c1, c2), VALUES('a', 'b') AS TB(ca, cb);
{ y } 2 a b

-- Return the first not-NULL column in TA
SELECT coalesce(TA.*)  FROM VALUES(1, 2) AS TA(c1, c2), VALUES('a', 'b') AS TB(ca, cb);
1

-- Return 1 if any column in TB contains a 'c'.
SELECT CASE WHEN 'c' IN (TB.*) THEN 1 END FROM VALUES(1, 2) AS TA(c1, c2), VALUES('a', 'b') AS TB(ca, cb);
NULL

-- Return all column as a single struct
SELECT (*) FROM VALUES(1, 2) AS TA(c1, c2), VALUES('a', 'b') AS TB(ca, cb);
{ c1: 1, c2: 2, ca: a, cb: b }

-- Flatten a struct into individual columns
SELECT c1.* FROM VALUES(named_struct('x', 1, 'y', 2)) AS TA(c1);
1  2
```

### Related

* [SELECT Main](sql-ref-syntax-qry-select.html)
* [WHERE Clause](sql-ref-syntax-qry-select-where.html)
