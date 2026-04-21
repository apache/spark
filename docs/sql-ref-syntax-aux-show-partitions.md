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
SHOW PARTITIONS table_identifier [ partition_spec ] [ AS JSON ]
```

### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    An optional parameter that specifies a comma separated list of key and value pairs
    for partitions. When specified, the partitions that match the partition specification are returned.

    **Syntax:** `PARTITION ( partition_col_name = partition_col_val [ , ... ] )`

* **AS JSON**

    An optional parameter to return the partition list as a single-row JSON document
    instead of the default tabular format. Only supported for V1 (session catalog / Hive
    metastore) tables.

    **Syntax:** `[ AS JSON ]`

    **Output schema:** A single column named `json_metadata` of type `STRING NOT NULL`.

    **Schema:**

    Below is the full JSON schema.
    In actual output, the JSON is not pretty-printed (see Examples).

    ```json
    {
      "partitions": ["<partition_string>", ...]
    }
    ```

    | Field | Type | Description |
    |---|---|---|
    | `partitions` | array of strings | Each element is a partition path string in `col=val/col=val` format. The array is empty when no partitions match the optional `partition_spec`. |

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

-- List all partitions as a JSON
SHOW PARTITIONS customer AS JSON;
+----------------------------------------------------------------------------------------+
|json_metadata                                                                           |
+----------------------------------------------------------------------------------------+
|{"partitions":["state=AZ/city=Peoria","state=CA/city=Fremont","state=CA/city=San Jose"]}|
+----------------------------------------------------------------------------------------+

-- Filter with a partial partition spec and return results as JSON
SHOW PARTITIONS customer PARTITION (state = 'CA') AS JSON;
+------------------------------------------------------------------+
|json_metadata                                                     |
+------------------------------------------------------------------+
|{"partitions":["state=CA/city=Fremont","state=CA/city=San Jose"]} |
+------------------------------------------------------------------+

-- Filter with a full partition spec as JSON
SHOW PARTITIONS customer PARTITION (state = 'CA', city = 'Fremont') AS JSON;
+-----------------------------------------+
|json_metadata                            |
+-----------------------------------------+
|{"partitions":["state=CA/city=Fremont"]} |
+-----------------------------------------+

-- When no partitions match the spec, AS JSON returns an empty array
SHOW PARTITIONS customer PARTITION (state = 'TX') AS JSON;
+------------------+
|json_metadata     |
+------------------+
|{"partitions":[]} |
+------------------+
```

### Related Statements

* [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
* [INSERT STATEMENT](sql-ref-syntax-dml-insert-table.html)
* [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
* [SHOW TABLE](sql-ref-syntax-aux-show-table.html)
