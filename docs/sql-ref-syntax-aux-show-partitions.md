---
layout: global
title: SHOW PARTITIONS
displayTitle: SHOW PARTITIONS
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

The `SHOW PARTITIONS` statement is used to list partitions of a table. An optional
partition spec may be specified to return the partitions matching the supplied
partition spec.

### Syntax

```sql
SHOW PARTITIONS table_identifier [ partition_spec ]
```

### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    An optional parameter that specifies a comma separated list of key and value pairs
    for partitions. When specified, the partitions that match the partition specification are returned.

    **Syntax:** `PARTITION ( partition_col_name = partition_col_val [ , ... ] )`

### Examples

```sql
-- create a partitioned table and insert a few rows.
USE salesdb;
CREATE TABLE customer(id INT, name STRING) PARTITIONED BY (state STRING, city STRING);
INSERT INTO customer PARTITION (state = 'CA', city = 'Fremont') VALUES (100, 'John');
INSERT INTO customer PARTITION (state = 'CA', city = 'San Jose') VALUES (200, 'Marry');
INSERT INTO customer PARTITION (state = 'AZ', city = 'Peoria') VALUES (300, 'Daniel');

-- Lists all partitions for table `customer`
SHOW PARTITIONS customer;
+----------------------+
|             partition|
+----------------------+
|  state=AZ/city=Peoria|
| state=CA/city=Fremont|
|state=CA/city=San Jose|
+----------------------+

-- Lists all partitions for the qualified table `customer`
SHOW PARTITIONS salesdb.customer;
+----------------------+
|             partition|
+----------------------+
|  state=AZ/city=Peoria|
| state=CA/city=Fremont|
|state=CA/city=San Jose|
+----------------------+

-- Specify a full partition spec to list specific partition
SHOW PARTITIONS customer PARTITION (state = 'CA', city = 'Fremont');
+---------------------+
|            partition|
+---------------------+
|state=CA/city=Fremont|
+---------------------+

-- Specify a partial partition spec to list the specific partitions
SHOW PARTITIONS customer PARTITION (state = 'CA');
+----------------------+
|             partition|
+----------------------+
| state=CA/city=Fremont|
|state=CA/city=San Jose|
+----------------------+

-- Specify a partial spec to list specific partition
SHOW PARTITIONS customer PARTITION (city =  'San Jose');
+----------------------+
|             partition|
+----------------------+
|state=CA/city=San Jose|
+----------------------+
```

### Related Statements

* [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
* [INSERT STATEMENT](sql-ref-syntax-dml-insert.html)
* [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
* [SHOW TABLE](sql-ref-syntax-aux-show-table.html)
