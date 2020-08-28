---
layout: global
title: REFRESH
displayTitle: REFRESH
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

`REFRESH` is used to invalidate and refresh all the cached data (and the associated metadata) for
all Datasets that contains the given data source path. Path matching is by prefix, i.e. "/" would
invalidate everything that is cached. 

### Syntax

```sql
REFRESH resource_path
```

### Parameters

* **resource_path**

    The path of the resource that is to be refreshed.

### Examples

```sql
-- The Path is resolved using the datasource's File Index.
CREATE TABLE test(ID INT) using parquet;
INSERT INTO test SELECT 1000;
CACHE TABLE test;
INSERT INTO test SELECT 100;
REFRESH "hdfs://path/to/table";
```

### Related Statements

* [CACHE TABLE](sql-ref-syntax-aux-cache-cache-table.html)
* [CLEAR CACHE](sql-ref-syntax-aux-cache-clear-cache.html)
* [UNCACHE TABLE](sql-ref-syntax-aux-cache-uncache-table.html)
* [REFRESH TABLE](sql-ref-syntax-aux-cache-refresh-table.html)
* [REFRESH FUNCTION](sql-ref-syntax-aux-cache-refresh-function.html)
