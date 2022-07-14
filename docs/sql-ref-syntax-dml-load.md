---
layout: global
title: LOAD DATA
displayTitle: LOAD DATA
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

`LOAD DATA` statement loads the data into a Hive serde table from the user specified directory or file. If a directory is specified then all the files from the directory are loaded. If a file is specified then only the single file is loaded. Additionally the `LOAD DATA` statement takes an optional partition specification. When a partition is specified, the data files (when input source is a directory) or the single file (when input source is a file) are loaded into the partition of the target table.

If the table is cached, the command clears cached data of the table and all its dependents that refer to it. The cache will be lazily filled when the next time the table or the dependents are accessed.

### Syntax

```sql
LOAD DATA [ LOCAL ] INPATH path [ OVERWRITE ] INTO TABLE table_identifier [ partition_spec ]
```

### Parameters

* **path**

    Path of the file system. It can be either an absolute or a relative path.

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    An optional parameter that specifies a comma separated list of key and value pairs
    for partitions.

    **Syntax:** `PARTITION ( partition_col_name = partition_col_val [ , ... ] )`

* **LOCAL**

    If specified, it causes the `INPATH` to be resolved against the local file system, instead of the default file system, which is typically a distributed storage.

* **OVERWRITE**

    By default, new data is appended to the table. If `OVERWRITE` is used, the table is instead overwritten with new data.

### Examples

```sql
-- Example without partition specification.
-- Assuming the students table has already been created and populated.
SELECT * FROM students;
+---------+----------------------+----------+
|     name|               address|student_id|
+---------+----------------------+----------+
|Amy Smith|123 Park Ave, San Jose|    111111|
+---------+----------------------+----------+

CREATE TABLE test_load (name VARCHAR(64), address VARCHAR(64), student_id INT) USING HIVE;

-- Assuming the students table is in '/user/hive/warehouse/'
LOAD DATA LOCAL INPATH '/user/hive/warehouse/students' OVERWRITE INTO TABLE test_load;

SELECT * FROM test_load;
+---------+----------------------+----------+
|     name|               address|student_id|
+---------+----------------------+----------+
|Amy Smith|123 Park Ave, San Jose|    111111|
+---------+----------------------+----------+

-- Example with partition specification.
CREATE TABLE test_partition (c1 INT, c2 INT, c3 INT) PARTITIONED BY (c2, c3);

INSERT INTO test_partition PARTITION (c2 = 2, c3 = 3) VALUES (1);

INSERT INTO test_partition PARTITION (c2 = 5, c3 = 6) VALUES (4);

INSERT INTO test_partition PARTITION (c2 = 8, c3 = 9) VALUES (7);

SELECT * FROM test_partition;
+---+---+---+
| c1| c2| c3|
+---+---+---+
|  1|  2|  3|
|  4|  5|  6|
|  7|  8|  9|
+---+---+---+

CREATE TABLE test_load_partition (c1 INT, c2 INT, c3 INT) USING HIVE PARTITIONED BY (c2, c3);

-- Assuming the test_partition table is in '/user/hive/warehouse/'
LOAD DATA LOCAL INPATH '/user/hive/warehouse/test_partition/c2=2/c3=3'
    OVERWRITE INTO TABLE test_load_partition PARTITION (c2=2, c3=3);

SELECT * FROM test_load_partition;
+---+---+---+
| c1| c2| c3|
+---+---+---+
|  1|  2|  3|
+---+---+---+
```
