---
layout: global
title: SHOW DATABASES
displayTitle: SHOW DATABASES
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

Lists the databases that match an optionally supplied regular expression pattern. If no
pattern is supplied then the command lists all the databases in the system.
Please note that the usage of `SCHEMAS` and `DATABASES` are interchangeable
and mean the same thing.

### Syntax

```sql
SHOW { DATABASES | SCHEMAS } [ LIKE regex_pattern ]
```

### Parameters

* **regex_pattern**

    Specifies a regular expression pattern that is used to filter the results of the
    statement.
    * Except for `*` and `|` character, the pattern works like a regular expression.
    * `*` alone matches 0 or more characters and `|` is used to separate multiple different regular expressions,
       any of which can match.
    * The leading and trailing blanks are trimmed in the input pattern before processing. The pattern match is case-insensitive.

### Examples

```sql
-- Create database. Assumes a database named `default` already exists in
-- the system. 
CREATE DATABASE payroll_db;
CREATE DATABASE payments_db;

-- Lists all the databases. 
SHOW DATABASES;
+------------+
|databaseName|
+------------+
|     default|
| payments_db|
|  payroll_db|
+------------+
  
-- Lists databases with name starting with string pattern `pay`
SHOW DATABASES LIKE 'pay*';
+------------+
|databaseName|
+------------+
| payments_db|
|  payroll_db|
+------------+
  
-- Lists all databases. Keywords SCHEMAS and DATABASES are interchangeable. 
SHOW SCHEMAS;
+------------+
|databaseName|
+------------+
|     default|
| payments_db|
|  payroll_db|
+------------+
```

### Related Statements

* [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
* [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
* [ALTER DATABASE](sql-ref-syntax-ddl-alter-database.html)
