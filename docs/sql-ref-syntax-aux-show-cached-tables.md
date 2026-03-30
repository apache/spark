---
layout: global
title: SHOW CACHED TABLES
displayTitle: SHOW CACHED TABLES
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

The `SHOW CACHED TABLES` statement returns every in-memory cache entry that was registered with an explicit table or view name, for example via [`CACHE TABLE`](sql-ref-syntax-aux-cache-cache-table.html) or `spark.catalog.cacheTable`. The result has two columns: `tableName` (the name used when caching) and `storageLevel` (a string description of how the data is cached).

Relations cached only through `Dataset.cache()` / `DataFrame.cache()` without assigning a catalog name are **not** listed.

### Syntax

```sql
SHOW CACHED TABLES
```

### Examples

```sql
CACHE TABLE my_table AS SELECT * FROM src;

SHOW CACHED TABLES;
+----------+--------------------------------------+
| tableName|                          storageLevel|
+----------+--------------------------------------+
|  my_table|Disk Memory Deserialized 1x Replicated|
+----------+--------------------------------------+
```

### Related Statements

* [CACHE TABLE](sql-ref-syntax-aux-cache-cache-table.html)
* [UNCACHE TABLE](sql-ref-syntax-aux-cache-uncache-table.html)
* [CLEAR CACHE](sql-ref-syntax-aux-cache-clear-cache.html)
