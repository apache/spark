---
layout: global
title: Table-valued Functions (TVF)
displayTitle: Table-valued Functions (TVF)
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

A table-valued function (TVF) is a function that returns a relation or a set of rows. There are two types of TVFs in Spark SQL: 1) A TVF that can be specified in a FROM clause, e.g. range; 2) A TVF that can be specified in a SELECT clause, e.g. explode.

### Supported Table-valued Functions

|Function|Argument Type(s)|Description|
|--------|----------------|-----------|
|**range** ( *end* )|Long|Creates a table with a single *LongType* column named *id*, <br/> containing rows in a range from 0 to *end* (exclusive) with step value 1.|
|**range** ( *start, end* )|Long, Long|Creates a table with a single *LongType* column named *id*, <br/> containing rows in a range from *start* to *end* (exclusive) with step value 1.|
|**range** ( *start, end, step* )|Long, Long, Long|Creates a table with a single *LongType* column named *id*, <br/> containing rows in a range from *start* to *end* (exclusive) with *step* value.|
|**range** ( *start, end, step, numPartitions* )|Long, Long, Long, Int|Creates a table with a single *LongType* column named *id*, <br/> containing rows in a range from *start* to *end* (exclusive) with *step* value, with partition number *numPartitions* specified.|
|**explode** ( *expr* )|Expression|Separates the elements of array *expr* into multiple rows, or the elements of map *expr* into multiple rows and columns. Unless specified otherwise, uses the default column name col for elements of the array or key and value for the elements of the map.|
|**explode_outer** <br> ( *expr* )|Expression|Separates the elements of array *expr* into multiple rows, or the elements of map *expr* into multiple rows and columns. Unless specified otherwise, uses the default column name col for elements of the array or key and value for the elements of the map.|
|**inline** ( *expr* )|Expression|Explodes an array of structs into a table. Uses column names col1, col2, etc. by default unless specified otherwise.|
|**inline_outer** <br> ( *expr* )|Expression|Explodes an array of structs into a table. Uses column names col1, col2, etc. by default unless specified otherwise.|
|**posexplode** <br> ( *expr* )|Expression|Separates the elements of array *expr* into multiple rows with positions, or the elements of map *expr* into multiple rows and columns with positions. Unless specified otherwise, uses the column name pos for position, col for elements of the array or key and value for elements of the map.|
|**posexplode_outer** ( *expr* )|Expression|Separates the elements of array *expr* into multiple rows with positions, or the elements of map *expr* into multiple rows and columns with positions. Unless specified otherwise, uses the column name pos for position, col for elements of the array or key and value for elements of the map.|
|**stack** ( *n, expr1, ..., exprk* )|Seq[Expression]|Separates *expr1, ..., exprk* into n rows. Uses column names col0, col1, etc. by default unless specified otherwise.|

### Examples

```sql
-- range call with end
SELECT * FROM range(6 + cos(3));
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+

-- range call with start and end
SELECT * FROM range(5, 10);
+---+
| id|
+---+
|  5|
|  6|
|  7|
|  8|
|  9|
+---+

-- range call with numPartitions
SELECT * FROM range(0, 10, 2, 200);
+---+
| id|
+---+
|  0|
|  2|
|  4|
|  6|
|  8|
+---+

-- range call with a table alias
SELECT * FROM range(5, 8) AS test;
+---+
| id|
+---+
|  5|
|  6|
|  7|
+---+

SELECT explode(array(10, 20));
+---+
|col|
+---+
| 10|
| 20|
+---+

SELECT inline(array(struct(1, 'a'), struct(2, 'b')));
+----+----+
|col1|col2|
+----+----+
|   1|   a|
|   2|   b|
+----+----+

SELECT posexplode(array(10,20));
+---+---+
|pos|col|
+---+---+
|  0| 10|
|  1| 20|
+---+---+

SELECT stack(2, 1, 2, 3);
+----+----+
|col0|col1|
+----+----+
|   1|   2|
|   3|null|
+----+----+

```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
