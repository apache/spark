---
layout: global
title: SHOW COLUMNS
displayTitle: SHOW COLUMNS
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

Returns the list of columns in a table. If the table does not exist, an exception is thrown.

### Syntax

```sql
SHOW COLUMNS table_identifier [ database ]
```

### Parameters

* **table_identifier**

    Specifies the table name of an existing table. The table may be optionally qualified
    with a database name.

    **Syntax:** `{ IN | FROM } [ database_name . ] table_name`

    **Note:** Keywords `IN` and `FROM` are interchangeable.

* **database**

    Specifies an optional database name. The table is resolved from this database when it
    is specified. When this parameter is specified then table
    name should not be qualified with a different database name. 

    **Syntax:** `{ IN | FROM } database_name`

    **Note:** Keywords `IN` and `FROM` are interchangeable.

### Examples

```sql
-- Create `customer` table in `salesdb` database;
USE salesdb;
CREATE TABLE customer(
    cust_cd INT,
    name VARCHAR(100),
    cust_addr STRING);

-- List the columns of `customer` table in current database.
SHOW COLUMNS IN customer;
+---------+
| col_name|
+---------+
|  cust_cd|
|     name|
|cust_addr|
+---------+

-- List the columns of `customer` table in `salesdb` database.
SHOW COLUMNS IN salesdb.customer;
+---------+
| col_name|
+---------+
|  cust_cd|
|     name|
|cust_addr|
+---------+

-- List the columns of `customer` table in `salesdb` database
SHOW COLUMNS IN customer IN salesdb;
+---------+
| col_name|
+---------+
|  cust_cd|
|     name|
|cust_addr|
+---------+
```

### Related Statements

* [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
* [SHOW TABLE](sql-ref-syntax-aux-show-table.html)
