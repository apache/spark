---
layout: global
title: REFRESH FUNCTION
displayTitle: REFRESH FUNCTION
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

`REFRESH FUNCTION` statement invalidates the cached function entry, which includes a class name
and resource location of the given function. The invalidated cache is populated right away.
Note that `REFRESH FUNCTION` only works for permanent functions. Refreshing native functions or temporary functions will cause an exception.

### Syntax

```sql
REFRESH FUNCTION function_identifier
```

### Parameters

* **function_identifier**

    Specifies a function name, which is either a qualified or unqualified name. If no database identifier is provided, uses the current database.

    **Syntax:** `[ database_name. ] function_name`

### Examples

```sql
-- The cached entry of the function will be refreshed
-- The function is resolved from the current database as the function name is unqualified.
REFRESH FUNCTION func1;

-- The cached entry of the function will be refreshed
-- The function is resolved from tempDB database as the function name is qualified.
REFRESH FUNCTION db1.func1;   
```

### Related Statements

* [CACHE TABLE](sql-ref-syntax-aux-cache-cache-table.html)
* [CLEAR CACHE](sql-ref-syntax-aux-cache-clear-cache.html)
* [UNCACHE TABLE](sql-ref-syntax-aux-cache-uncache-table.html)
* [REFRESH TABLE](sql-ref-syntax-aux-cache-refresh-table.html)
* [REFRESH](sql-ref-syntax-aux-cache-refresh.html)
