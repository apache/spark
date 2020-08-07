---
layout: global
title: DESCRIBE TABLE
displayTitle: DESCRIBE TABLE
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

`DESCRIBE TABLE` statement returns the basic metadata information of a
table. The metadata information includes column name, column type
and column comment. Optionally a partition spec or column name may be specified
to return the metadata pertaining to a partition or column respectively.

### Syntax

```sql
{ DESC | DESCRIBE } [ TABLE ] [ format ] table_identifier [ partition_spec ] [ col_name ]
```

### Parameters

* **format**

    Specifies the optional format of describe output. If `EXTENDED` is specified
    then additional metadata information (such as parent database, owner, and access time)
    is returned. 

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    An optional parameter that specifies a comma separated list of key and value pairs
    for partitions. When specified, additional partition metadata is returned.

    **Syntax:** `PARTITION ( partition_col_name  = partition_col_val [ , ... ] )`

* **col_name**

    An optional parameter that specifies the column name that needs to be described.
    The supplied column name may be optionally qualified. Parameters `partition_spec`
    and `col_name` are  mutually exclusive and can not be specified together. Currently
    nested columns are not allowed to be specified.
    
    **Syntax:** `[ database_name. ] [ table_name. ] column_name`

### Examples

```sql
-- Creates a table `customer`. Assumes current database is `salesdb`.
CREATE TABLE customer(
        cust_id INT,
        state VARCHAR(20),
        name STRING COMMENT 'Short name'
    )
    USING parquet
    PARTITIONED BY (state);
    
INSERT INTO customer PARTITION (state = 'AR') VALUES (100, 'Mike');
    
-- Returns basic metadata information for unqualified table `customer`
DESCRIBE TABLE customer;
+-----------------------+---------+----------+
|               col_name|data_type|   comment|
+-----------------------+---------+----------+
|                cust_id|      int|      null|
|                   name|   string|Short name|
|                  state|   string|      null|
|# Partition Information|         |          |
|             # col_name|data_type|   comment|
|                  state|   string|      null|
+-----------------------+---------+----------+

-- Returns basic metadata information for qualified table `customer`
DESCRIBE TABLE salesdb.customer;
+-----------------------+---------+----------+
|               col_name|data_type|   comment|
+-----------------------+---------+----------+
|                cust_id|      int|      null|
|                   name|   string|Short name|
|                  state|   string|      null|
|# Partition Information|         |          |
|             # col_name|data_type|   comment|
|                  state|   string|      null|
+-----------------------+---------+----------+

-- Returns additional metadata such as parent database, owner, access time etc.
DESCRIBE TABLE EXTENDED customer;
+----------------------------+------------------------------+----------+
|                    col_name|                     data_type|   comment|
+----------------------------+------------------------------+----------+
|                     cust_id|                           int|      null|
|                        name|                        string|Short name|
|                       state|                        string|      null|
|     # Partition Information|                              |          |
|                  # col_name|                     data_type|   comment|
|                       state|                        string|      null|
|                            |                              |          |
|# Detailed Table Information|                              |          |
|                    Database|                       default|          |
|                       Table|                      customer|          |
|                       Owner|                 <TABLE OWNER>|          |
|                Created Time|  Tue Apr 07 22:56:34 JST 2020|          |
|                 Last Access|                       UNKNOWN|          |
|                  Created By|               <SPARK VERSION>|          |
|                        Type|                       MANAGED|          |
|                    Provider|                       parquet|          |
|                    Location|file:/tmp/salesdb.db/custom...|          |
|               Serde Library|org.apache.hadoop.hive.ql.i...|          |
|                 InputFormat|org.apache.hadoop.hive.ql.i...|          |
|                OutputFormat|org.apache.hadoop.hive.ql.i...|          |
|          Partition Provider|                       Catalog|          |
+----------------------------+------------------------------+----------+

-- Returns partition metadata such as partitioning column name, column type and comment.
DESCRIBE TABLE EXTENDED customer PARTITION (state = 'AR');
+------------------------------+------------------------------+----------+
|                      col_name|                     data_type|   comment|
+------------------------------+------------------------------+----------+
|                       cust_id|                           int|      null|
|                          name|                        string|Short name|
|                         state|                        string|      null|
|       # Partition Information|                              |          |
|                    # col_name|                     data_type|   comment|
|                         state|                        string|      null|
|                              |                              |          |
|# Detailed Partition Inform...|                              |          |
|                      Database|                       default|          |
|                         Table|                      customer|          |
|              Partition Values|                    [state=AR]|          |
|                      Location|file:/tmp/salesdb.db/custom...|          |
|                 Serde Library|org.apache.hadoop.hive.ql.i...|          |
|                   InputFormat|org.apache.hadoop.hive.ql.i...|          |
|                  OutputFormat|org.apache.hadoop.hive.ql.i...|          |
|            Storage Properties|[serialization.format=1, pa...|          |
|          Partition Parameters|{transient_lastDdlTime=1586...|          |
|                  Created Time|  Tue Apr 07 23:05:43 JST 2020|          |
|                   Last Access|                       UNKNOWN|          |
|          Partition Statistics|                     659 bytes|          |
|                              |                              |          |
|         # Storage Information|                              |          |
|                      Location|file:/tmp/salesdb.db/custom...|          |
|                 Serde Library|org.apache.hadoop.hive.ql.i...|          |
|                   InputFormat|org.apache.hadoop.hive.ql.i...|          |
|                  OutputFormat|org.apache.hadoop.hive.ql.i...|          |
+------------------------------+------------------------------+----------+

-- Returns the metadata for `name` column.
-- Optional `TABLE` clause is omitted and column is fully qualified.
DESCRIBE customer salesdb.customer.name;
+---------+----------+
|info_name|info_value|
+---------+----------+
| col_name|      name|
|data_type|    string|
|  comment|Short name|
+---------+----------+
```

### Related Statements

* [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
* [DESCRIBE QUERY](sql-ref-syntax-aux-describe-query.html)
* [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
