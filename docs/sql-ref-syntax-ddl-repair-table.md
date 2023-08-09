---
layout: global
title: REPAIR TABLE
displayTitle: REPAIR TABLE
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

`REPAIR TABLE` recovers all the partitions in the directory of a table and updates the Hive metastore. When creating a table using `PARTITIONED BY` clause, partitions are generated and registered in the Hive metastore. However, if the partitioned table is created from existing data, partitions are not registered automatically in the Hive metastore. User needs to run `REPAIR TABLE` to register the partitions. `REPAIR TABLE` on a non-existent table or a table without partitions throws an exception. Another way to recover partitions is to use `ALTER TABLE RECOVER PARTITIONS`. This command can also be invoked using `MSCK REPAIR TABLE`, for Hive compatibility.

If the table is cached, the command clears cached data of the table and all its dependents that refer to it. The cache will be lazily filled when the next time the table or the dependents are accessed.

### Syntax

```sql
[MSCK] REPAIR TABLE table_identifier [{ADD|DROP|SYNC} PARTITIONS]
```

### Parameters

* **table_identifier**

    Specifies the name of the table to be repaired. The table name may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **`{ADD|DROP|SYNC} PARTITIONS`**

    Specifies how to recover partitions. If not specified, **ADD** is the default.

    * **ADD**, the command adds new partitions to the session catalog for all sub-folder in the base table folder that don't belong to any table partitions.
    * **DROP**, the command drops all partitions from the session catalog that have non-existing locations in the file system.
    * **SYNC** is the combination of **DROP** and **ADD**. 

### Examples

```sql
-- create a partitioned table from existing data /tmp/namesAndAges.parquet
CREATE TABLE t1 (name STRING, age INT) USING parquet PARTITIONED BY (age)
    LOCATION "/tmp/namesAndAges.parquet";

-- SELECT * FROM t1 does not return results
SELECT * FROM t1;

-- run REPAIR TABLE to recovers all the partitions
REPAIR TABLE t1;

-- SELECT * FROM t1 returns results
SELECT * FROM t1;
+-------+---+
|   name|age|
+-------+---+
|Michael| 20|
+-------+---+
| Justin| 19|
+-------+---+
|   Andy| 30|
+-------+---+
```

### Related Statements

* [ALTER TABLE](sql-ref-syntax-ddl-alter-table.html)
