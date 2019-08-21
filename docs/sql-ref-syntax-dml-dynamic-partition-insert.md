---
layout: global
title: Dynamic Partition Inserts
displayTitle: Dynamic Partition Inserts
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

When the partition value is not provided, such inserts are called as the dynamic partition inserts, also called as multi-partition inserts. In partition spec, the partition column values are optional. When the values are not given, these columns are referred to as dynamic partition columns; otherwise, they are static partition columns. For example, the partition spec (p1 = 3, p2, p3) has a static partition column (p1) and two dynamic partition columns (p2 and p3).

In partition spec, the static partition keys must come before the dynamic partition keys. That means, all partition columns having constant values need to appear before other partition columns that do not have an assigned constant value.

The partition values of dynamic partition columns are determined during the execution. The dynamic partition columns must be specified last in both partition spec and the input result set (of the row value lists or the select query). They are resolved by position, instead of by names. Thus, the orders must be exactly matched.

Currently the DataFrameWriter APIs do not have an interface to specify partition values. Therefore, its `insertInto()` API is always using dynamic partition mode.

> **<span style="color:red">IMPORTANT</span>**
>
> In the dynamic partition mode, the input result set could result in a large number of dynamic partitions, and thus generate a large number of partition directories.
>
> **OVERWRITE**
>
> The semantics are different based on the type of the target table.
>
>  * Hive SerDe tables: INSERT OVERWRITE doesn’t delete partitions ahead, and only overwrite those partitions that have data written into it at runtime. This matches Apache Hive semantics. For Hive SerDe tables, Spark SQL respects the Hive-related configuration, including hive.exec.dynamic.partition and hive.exec.dynamic.partition.mode.
>  * Native data source tables: INSERT OVERWRITE first deletes all the partitions that match the partition specification (e.g., PARTITION(a=1, b)) and then inserts all the remaining values. Since Databricks Runtime 3.2, the behavior of native data source tables can be changed to be consistent with Hive SerDe tables by changing the session-specific configuration spark.sql.sources.partitionOverwriteMode to DYNAMIC. The default mode is STATIC.


### Examples
{% highlight sql %}
-- Create a partitioned native Parquet table
CREATE TABLE data_source_tab2 (col1 INT, p1 STRING, p2 STRING)
  USING PARQUET PARTITIONED BY (p1, p2)

-- Two partitions ('part1', 'part1') and ('part1', 'part2') are created by this dynamic insert.
-- The dynamic partition column p2 is resolved by the last column `'part' || id`
INSERT INTO data_source_tab2 PARTITION (p1 = 'part1', p2)
  SELECT id, 'part' || id FROM RANGE(1, 3)

-- A new partition ('partNew1', 'partNew2') is added by this INSERT OVERWRITE.
INSERT OVERWRITE TABLE data_source_tab2 PARTITION (p1 = 'partNew1', p2)
  VALUES (3, 'partNew2')

-- After this INSERT OVERWRITE, the two partitions ('part1', 'part1') and ('part1', 'part2')
-- are dropped,
-- because both partitions are included by (p1 = 'part1', p2).
-- Then, two partitions ('partNew1', 'partNew2'), ('part1', 'part1') exist after this
-- operation.
INSERT OVERWRITE TABLE data_source_tab2 PARTITION (p1 = 'part1', p2)
  VALUES (5, 'part1')

-- Create and fill a partitioned hive serde table with three partitions:
-- ('part1', 'part1'), ('part1', 'part2') and ('partNew1', 'partNew2')
CREATE TABLE hive_serde_tab2 (col1 INT, p1 STRING, p2 STRING)
  USING HIVE OPTIONS(fileFormat 'PARQUET') PARTITIONED BY (p1, p2)
INSERT INTO hive_serde_tab2 PARTITION (p1 = 'part1', p2)
  SELECT id, 'part' || id FROM RANGE(1, 3)
INSERT OVERWRITE TABLE hive_serde_tab2 PARTITION (p1 = 'partNew1', p2)
  VALUES (3, 'partNew2')

-- After this INSERT OVERWRITE, only the partitions ('part1', 'part1') is overwritten by the
-- new value.
-- All the three partitions still exist.
INSERT OVERWRITE TABLE hive_serde_tab2 PARTITION (p1 = 'part1', p2)
  VALUES (5, 'part1')
{% endhighlight %}