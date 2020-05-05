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

A table-valued function (TVF) is a function that returns a relation or a set of rows.

### Syntax

```sql
function_name ( expression [ , ... ] ) [ table_alias ]
```

### Parameters

* **expression**

    Specifies a combination of one or more values, operators and SQL functions that results in a value.

* **table_alias**

    Specifies a temporary name with an optional column name list.

    **Syntax:** `[ AS ] table_name [ ( column_name [ , ... ] ) ]`

### Supported Table-valued Functions

|Function|Argument Type(s)|Description|
|--------|----------------|-----------|
|**range** ( *end* )|Long|Creates a table with a single *LongType* column named *id*, <br/> containing rows in a range from 0 to *end* (exclusive) with step value 1.|
|**range** ( *start, end* )|Long, Long|Creates a table with a single *LongType* column named *id*, <br/> containing rows in a range from *start* to *end* (exclusive) with step value 1.|
|**range** ( *start, end, step* )|Long, Long, Long|Creates a table with a single *LongType* column named *id*, <br/> containing rows in a range from *start* to *end* (exclusive) with *step* value.|
|**range** ( *start, end, step, numPartitions* )|Long, Long, Long, Int|Creates a table with a single *LongType* column named *id*, <br/> containing rows in a range from *start* to *end* (exclusive) with *step* value, with partition number *numPartitions* specified.|

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
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
