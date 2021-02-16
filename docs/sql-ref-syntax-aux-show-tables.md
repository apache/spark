---
layout: global
title: SHOW TABLES
displayTitle: SHOW TABLES
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

The `SHOW TABLES` statement returns all the tables for an optionally specified database.
Additionally, the output of this statement may be filtered by an optional matching
pattern. If no database is specified then the tables are returned from the 
current database.

### Syntax

```sql
SHOW TABLES [ { FROM | IN } database_name ] [ LIKE regex_pattern ]
```

### Parameters

* **{ FROM `|` IN } database_name**

     Specifies the database name from which tables are listed.

* **regex_pattern**

     Specifies the regular expression pattern that is used to filter out unwanted tables. 

     * Except for `*` and `|` character, the pattern works like a regular expression.
     * `*` alone matches 0 or more characters and `|` is used to separate multiple different regular expressions,
       any of which can match.
     * The leading and trailing blanks are trimmed in the input pattern before processing. The pattern match is case-insensitive.

### Examples

```sql
-- List all tables in default database
SHOW TABLES;
+--------+---------+-----------+
|database|tableName|isTemporary|
+--------+---------+-----------+
| default|      sam|      false|
| default|     sam1|      false|
| default|      suj|      false|
+--------+---------+-----------+

-- List all tables from userdb database 
SHOW TABLES FROM userdb;
+--------+---------+-----------+
|database|tableName|isTemporary|
+--------+---------+-----------+
|  userdb|    user1|      false|
|  userdb|    user2|      false|
+--------+---------+-----------+

-- List all tables in userdb database
SHOW TABLES IN userdb;
+--------+---------+-----------+
|database|tableName|isTemporary|
+--------+---------+-----------+
|  userdb|    user1|      false|
|  userdb|    user2|      false|
+--------+---------+-----------+

-- List all tables from default database matching the pattern `sam*`
SHOW TABLES FROM default LIKE 'sam*';
+--------+---------+-----------+
|database|tableName|isTemporary|
+--------+---------+-----------+
| default|      sam|      false|
| default|     sam1|      false|
+--------+---------+-----------+
  
-- List all tables matching the pattern `sam*|suj`
SHOW TABLES LIKE 'sam*|suj';
+--------+---------+-----------+
|database|tableName|isTemporary|
+--------+---------+-----------+
| default|      sam|      false|
| default|     sam1|      false|
| default|      suj|      false|
+--------+---------+-----------+
```

### Related Statements

* [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
* [DROP TABLE](sql-ref-syntax-ddl-drop-table.html)
* [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
* [DROP DATABASE](sql-ref-syntax-ddl-drop-database.html)
