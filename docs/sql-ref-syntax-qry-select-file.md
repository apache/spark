---
layout: global
title: File
displayTitle: File
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

You can query a file with a specified format directly with SQL.

### Syntax

```sql
file_format.`file_path`
```

### Parameters

* **file_format**

    Specifies a file format for a given file path, could be TEXTFILE, ORC, PARQUET, etc.

* **file_path**

    Specifies a file path with a given format.

### Examples

```sql
-- PARQUET file
SELECT * FROM parquet.`examples/src/main/resources/users.parquet`;
+------+--------------+----------------+
|  name|favorite_color|favorite_numbers|
+------+--------------+----------------+
|Alyssa|          null|  [3, 9, 15, 20]|
|   Ben|           red|              []|
+------+--------------+----------------+

-- ORC file
SELECT * FROM orc.`examples/src/main/resources/users.orc`;
+------+--------------+----------------+
|  name|favorite_color|favorite_numbers|
+------+--------------+----------------+
|Alyssa|          null|  [3, 9, 15, 20]|
|   Ben|           red|              []|
+------+--------------+----------------+

-- JSON file
SELECT * FROM json.`examples/src/main/resources/people.json`;
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
