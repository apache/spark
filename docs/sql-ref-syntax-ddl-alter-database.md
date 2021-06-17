---
layout: global
title: ALTER DATABASE
displayTitle: ALTER DATABASE
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

You can alter metadata associated with a database by setting `DBPROPERTIES`.  The specified property
values override any existing value with the same property name. Please note that the usage of 
`SCHEMA` and `DATABASE` are interchangeable and one can be used in place of the other. An error message
is issued if the database is not found in the system. This command is mostly used to record the metadata
for a database and may be used for auditing purposes.

### Syntax

```sql
ALTER { DATABASE | SCHEMA } database_name
    SET DBPROPERTIES ( property_name = property_value [ , ... ] )
```

### Parameters

* **database_name**

    Specifies the name of the database to be altered.

### Examples

```sql
-- Creates a database named `inventory`.
CREATE DATABASE inventory;

-- Alters the database to set properties `Edited-by` and `Edit-date`.
ALTER DATABASE inventory SET DBPROPERTIES ('Edited-by' = 'John', 'Edit-date' = '01/01/2001');

-- Verify that properties are set.
DESCRIBE DATABASE EXTENDED inventory;
+-------------------------+------------------------------------------+
|database_description_item|                database_description_value|
+-------------------------+------------------------------------------+
|            Database Name|                                 inventory|
|              Description|                                          |
|                 Location|   file:/temp/spark-warehouse/inventory.db|
|               Properties|((Edit-date,01/01/2001), (Edited-by,John))|
+-------------------------+------------------------------------------+
```

### Related Statements

* [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
