---
layout: global
title: REFRESH TABLE
displayTitle: REFRESH TABLE
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

`REFRESH TABLE` statement invalidates the cached entries, which include data
and metadata of the given table or view. The invalidated cache is populated in
lazy manner when the cached table or the query associated with it is executed again.

### Syntax

```sql
REFRESH [TABLE] table_identifier
```

### Parameters

* **table_identifier**

    Specifies a table name, which is either a qualified or unqualified name that designates a table/view. If no database identifier is provided, it refers to a temporary view or a table/view in the current database.

    **Syntax:** `[ database_name. ] table_name`

### Examples

```sql
-- The cached entries of the table will be refreshed  
-- The table is resolved from the current database as the table name is unqualified.
REFRESH TABLE tbl1;

-- The cached entries of the view will be refreshed or invalidated
-- The view is resolved from tempDB database, as the view name is qualified.
REFRESH TABLE tempDB.view1;   
```

### Related Statements

* [CACHE TABLE](sql-ref-syntax-aux-cache-cache-table.html)
* [CLEAR CACHE](sql-ref-syntax-aux-cache-clear-cache.html)
* [UNCACHE TABLE](sql-ref-syntax-aux-cache-uncache-table.html)
* [REFRESH](sql-ref-syntax-aux-cache-refresh.html)
* [REFRESH FUNCTION](sql-ref-syntax-aux-cache-refresh-function.html)