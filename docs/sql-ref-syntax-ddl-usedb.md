---
layout: global
title: USE Database
displayTitle: USE Database
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

`USE` statement is used to set the current database. After the current database is set,
the unqualified database artifacts such as tables, functions and views that are 
referenced by SQLs are resolved from the current database. 
The default database name is 'default'.

### Syntax

```sql
USE database_name
```

### Parameter

* **database_name**

    Name of the database will be used. If the database does not exist, an exception will be thrown.

### Examples

```sql
-- Use the 'userdb' which exists.
USE userdb;

-- Use the 'userdb1' which doesn't exist
USE userdb1;
Error: org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException: Database 'userdb1' not found;
(state=,code=0)
```

### Related Statements

* [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
* [DROP DATABASE](sql-ref-syntax-ddl-drop-database.html)
* [CREATE TABLE ](sql-ref-syntax-ddl-create-table.html)
