---
layout: global
title: DESCRIBE DATABASE
displayTitle: DESCRIBE DATABASE
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
​
`DESCRIBE DATABASE` statement returns the metadata of an existing database. The metadata information includes database
name, database comment, and database location on the filesystem. If the optional `EXTENDED` option is specified, it
returns the basic metadata information along with the database properties. The `DATABASE` and `SCHEMA` are
interchangeable.

### Syntax

```sql
{ DESC | DESCRIBE } DATABASE [ EXTENDED ] db_name
```

### Parameters

* **db_name**

    Specifies a name of an existing database or an existing schema in the system. If the name does not exist, an
    exception is thrown.

### Examples

```sql
-- Create employees DATABASE
CREATE DATABASE employees COMMENT 'For software companies';

-- Describe employees DATABASE.
-- Returns Database Name, Description and Root location of the filesystem
-- for the employees DATABASE.
DESCRIBE DATABASE employees;
+-------------------------+-----------------------------+
|database_description_item|   database_description_value|
+-------------------------+-----------------------------+
|            Database Name|                    employees|
|              Description|       For software companies|
|                 Location|file:/Users/Temp/employees.db|
+-------------------------+-----------------------------+

-- Create employees DATABASE
CREATE DATABASE employees COMMENT 'For software companies';

-- Alter employees database to set DBPROPERTIES
ALTER DATABASE employees SET DBPROPERTIES ('Create-by' = 'Kevin', 'Create-date' = '09/01/2019');

-- Describe employees DATABASE with EXTENDED option to return additional database properties
DESCRIBE DATABASE EXTENDED employees;
+-------------------------+---------------------------------------------+
|database_description_item|                   database_description_value|
+-------------------------+---------------------------------------------+
|            Database Name|                                    employees|
|              Description|                       For software companies|
|                 Location|                file:/Users/Temp/employees.db|
|               Properties|((Create-by,kevin), (Create-date,09/01/2019))|
+-------------------------+---------------------------------------------+

-- Create deployment SCHEMA
CREATE SCHEMA deployment COMMENT 'Deployment environment';

-- Describe deployment, the DATABASE and SCHEMA are interchangeable, their meaning are the same.
DESC DATABASE deployment;
+-------------------------+------------------------------+
|database_description_item|database_description_value    |
+-------------------------+------------------------------+
|            Database Name|                    deployment|
|              Description|        Deployment environment|
|                 Location|file:/Users/Temp/deployment.db|
+-------------------------+------------------------------+
```

### Related Statements

* [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
* [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
* [DESCRIBE QUERY](sql-ref-syntax-aux-describe-query.html)
