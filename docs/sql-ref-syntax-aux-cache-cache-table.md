---
layout: global
title: CACHE TABLE
displayTitle: CACHE TABLE
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

`CACHE TABLE` statement caches contents of a table or output of a query with the given storage level. If a query is cached, then a temp view will be created for this query.
This reduces scanning of the original files in future queries. 

### Syntax

```sql
CACHE [ LAZY ] TABLE table_identifier
    [ OPTIONS ( 'storageLevel' [ = ] value ) ] [ [ AS ] query ]
```

### Parameters

* **LAZY**

    Only cache the table when it is first used, instead of immediately.

* **table_identifier**

    Specifies the table or view name to be cached. The table or view name may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **OPTIONS ( 'storageLevel' [ = ] value )**

    `OPTIONS` clause with `storageLevel` key and value pair. A Warning is issued when a key other than `storageLevel` is used. The valid options for `storageLevel` are:
     * `NONE`
     * `DISK_ONLY`
     * `DISK_ONLY_2`
     * `DISK_ONLY_3`
     * `MEMORY_ONLY`
     * `MEMORY_ONLY_2`
     * `MEMORY_ONLY_SER`
     * `MEMORY_ONLY_SER_2`
     * `MEMORY_AND_DISK`
     * `MEMORY_AND_DISK_2`
     * `MEMORY_AND_DISK_SER`
     * `MEMORY_AND_DISK_SER_2`
     * `OFF_HEAP`

    An Exception is thrown when an invalid value is set for `storageLevel`. If `storageLevel` is not explicitly set using `OPTIONS` clause, the default `storageLevel` is set to `MEMORY_AND_DISK`.

* **query**

    A query that produces the rows to be cached. It can be in one of following formats:
    * a `SELECT` statement
    * a `TABLE` statement
    * a `FROM` statement

### Examples

```sql
CACHE TABLE testCache OPTIONS ('storageLevel' 'DISK_ONLY') SELECT * FROM testData;
```

### Related Statements

* [CLEAR CACHE](sql-ref-syntax-aux-cache-clear-cache.html)
* [UNCACHE TABLE](sql-ref-syntax-aux-cache-uncache-table.html)
* [REFRESH TABLE](sql-ref-syntax-aux-cache-refresh-table.html)
* [REFRESH](sql-ref-syntax-aux-cache-refresh.html)
* [REFRESH FUNCTION](sql-ref-syntax-aux-cache-refresh-function.html)