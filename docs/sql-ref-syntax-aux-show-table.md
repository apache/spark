---
layout: global
title: SHOW TABLE EXTENDED
displayTitle: SHOW TABLE EXTENDED
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

`SHOW TABLE EXTENDED` will show information for all tables matching the given regular expression.
Output includes basic table information and file system information like `Last Access`, 
`Created By`, `Type`, `Provider`, `Table Properties`, `Location`, `Serde Library`, `InputFormat`, 
`OutputFormat`, `Storage Properties`, `Partition Provider`, `Partition Columns` and `Schema`.

If a partition specification is present, it outputs the given partition's file-system-specific 
information such as `Partition Parameters` and `Partition Statistics`. Note that a table regex 
cannot be used with a partition specification.

### Syntax

```sql
SHOW TABLE EXTENDED [ { IN | FROM } database_name ] LIKE regex_pattern
    [ partition_spec ]
```

### Parameters

* **{ IN`|`FROM } database_name**

    Specifies database name. If not provided, will use the current database.

* **regex_pattern**

    Specifies the regular expression pattern that is used to filter out unwanted tables.

    * Except for `*` and `|` character, the pattern works like a regular expression.
    * `*` alone matches 0 or more characters and `|` is used to separate multiple different regular expressions,
      any of which can match.
    * The leading and trailing blanks are trimmed in the input pattern before processing. The pattern match is case-insensitive.

* **partition_spec**

    An optional parameter that specifies a comma separated list of key and value pairs
    for partitions. Note that a table regex cannot be used with a partition specification.

    **Syntax:** `PARTITION ( partition_col_name = partition_col_val [ , ... ] )`

### Examples

```sql
-- Assumes `employee` table created with partitioned by column `grade`
CREATE TABLE employee(name STRING, grade INT) PARTITIONED BY (grade);
INSERT INTO employee PARTITION (grade = 1) VALUES ('sam');
INSERT INTO employee PARTITION (grade = 2) VALUES ('suj');

 -- Show the details of the table
SHOW TABLE EXTENDED LIKE 'employee';
+--------+---------+-----------+--------------------------------------------------------------+
|database|tableName|isTemporary|                         information                          |
+--------+---------+-----------+--------------------------------------------------------------+
|default |employee |false      |Database: default
                                Table: employee
                                Owner: root
                                Created Time: Fri Aug 30 15:10:21 IST 2019
                                Last Access: Thu Jan 01 05:30:00 IST 1970
                                Created By: Spark 3.0.0-SNAPSHOT
                                Type: MANAGED
                                Provider: hive
                                Table Properties: [transient_lastDdlTime=1567158021]
                                Location: file:/opt/spark1/spark/spark-warehouse/employee
                                Serde Library: org.apache.hadoop.hive.serde2.lazy
                                .LazySimpleSerDe
                                InputFormat: org.apache.hadoop.mapred.TextInputFormat
                                OutputFormat: org.apache.hadoop.hive.ql.io
                                .HiveIgnoreKeyTextOutputFormat
                                Storage Properties: [serialization.format=1]
                                Partition Provider: Catalog
                                Partition Columns: [`grade`]
                                Schema: root
                                 |-- name: string (nullable = true)
                                 |-- grade: integer (nullable = true)
                                                                                                            
+--------+---------+-----------+--------------------------------------------------------------+

-- showing the multiple table details with pattern matching
SHOW TABLE EXTENDED  LIKE 'employe*';
+--------+---------+-----------+--------------------------------------------------------------+
|database|tableName|isTemporary|                         information                          |
+--------+---------+-----------+--------------------------------------------------------------+
|default |employee |false      |Database: default
                                Table: employee
                                Owner: root
                                Created Time: Fri Aug 30 15:10:21 IST 2019
                                Last Access: Thu Jan 01 05:30:00 IST 1970
                                Created By: Spark 3.0.0-SNAPSHOT
                                Type: MANAGED
                                Provider: hive
                                Table Properties: [transient_lastDdlTime=1567158021]
                                Location: file:/opt/spark1/spark/spark-warehouse/employee
                                Serde Library: org.apache.hadoop.hive.serde2.lazy
                                .LazySimpleSerDe
                                InputFormat: org.apache.hadoop.mapred.TextInputFormat
                                OutputFormat: org.apache.hadoop.hive.ql.io
                                .HiveIgnoreKeyTextOutputFormat
                                Storage Properties: [serialization.format=1]
                                Partition Provider: Catalog
                                Partition Columns: [`grade`]
                                Schema: root
                                 |-- name: string (nullable = true)
                                 |-- grade: integer (nullable = true)
  
|default |employee1|false      |Database: default
                                Table: employee1
                                Owner: root
                                Created Time: Fri Aug 30 15:22:33 IST 2019
                                Last Access: Thu Jan 01 05:30:00 IST 1970
                                Created By: Spark 3.0.0-SNAPSHOT
                                Type: MANAGED
                                Provider: hive
                                Table Properties: [transient_lastDdlTime=1567158753]
                                Location: file:/opt/spark1/spark/spark-warehouse/employee1
                                Serde Library: org.apache.hadoop.hive.serde2.lazy
                                .LazySimpleSerDe
                                InputFormat: org.apache.hadoop.mapred.TextInputFormat
                                OutputFormat: org.apache.hadoop.hive.ql.io
                                .HiveIgnoreKeyTextOutputFormat
                                Storage Properties: [serialization.format=1]
                                Partition Provider: Catalog
                                Schema: root
                                 |-- name: string (nullable = true)
                                                                                                               
+--------+---------+----------+---------------------------------------------------------------+
  
-- show partition file system details
SHOW TABLE EXTENDED  IN default LIKE 'employee' PARTITION (grade=1);
+--------+---------+-----------+--------------------------------------------------------------+
|database|tableName|isTemporary|                         information                          |
+--------+---------+-----------+--------------------------------------------------------------+
|default |employee |false      |Partition Values: [grade=1]
                                Location: file:/opt/spark1/spark/spark-warehouse/employee
                                /grade=1
                                Serde Library: org.apache.hadoop.hive.serde2.lazy
                                .LazySimpleSerDe
                                InputFormat: org.apache.hadoop.mapred.TextInputFormat
                                OutputFormat: org.apache.hadoop.hive.ql.io
                                .HiveIgnoreKeyTextOutputFormat
                                Storage Properties: [serialization.format=1]
                                Partition Parameters: {rawDataSize=-1, numFiles=1,
                                transient_lastDdlTime=1567158221, totalSize=4,
                                COLUMN_STATS_ACCURATE=false, numRows=-1}
                                Created Time: Fri Aug 30 15:13:41 IST 2019
                                Last Access: Thu Jan 01 05:30:00 IST 1970
                                Partition Statistics: 4 bytes
                                                                                                                                                                          |
+--------+---------+-----------+--------------------------------------------------------------+

-- show partition file system details with regex fails as shown below
SHOW TABLE EXTENDED  IN default LIKE 'empl*' PARTITION (grade=1);
Error: Error running query: org.apache.spark.sql.catalyst.analysis.NoSuchTableException:
 Table or view 'emplo*' not found in database 'default'; (state=,code=0)
```

### Related Statements

* [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
* [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
