---
layout: global
title: ALTER TABLE
displayTitle: ALTER TABLE
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

`ALTER TABLE` statement changes the schema or properties of a table.

### RENAME 

`ALTER TABLE RENAME TO` statement changes the table name of an existing table in the database.

#### Syntax

```sql
ALTER TABLE table_identifier RENAME TO table_identifier

ALTER TABLE table_identifier partition_spec RENAME TO partition_spec
```

#### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    Partition to be renamed.

    **Syntax:** `PARTITION ( partition_col_name  = partition_col_val [ , ... ] )`

### ADD COLUMNS

`ALTER TABLE ADD COLUMNS` statement adds mentioned columns to an existing table.

#### Syntax

```sql
ALTER TABLE table_identifier ADD COLUMNS ( col_spec [ , ... ] )
```

#### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **COLUMNS ( col_spec )**

    Specifies the columns to be added.

### ALTER OR CHANGE COLUMN

`ALTER TABLE ALTER COLUMN` or `ALTER TABLE CHANGE COLUMN` statement changes column's definition.

#### Syntax

```sql
ALTER TABLE table_identifier { ALTER | CHANGE } [ COLUMN ] col_spec alterColumnAction
```

#### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **COLUMNS ( col_spec )**

    Specifies the column to be altered or be changed.

* **alterColumnAction**

    Change column's definition.

### ADD AND DROP PARTITION

#### ADD PARTITION

`ALTER TABLE ADD` statement adds partition to the partitioned table.

##### Syntax

```sql
ALTER TABLE table_identifier ADD [IF NOT EXISTS] 
    ( partition_spec [ partition_spec ... ] )
```
     
##### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    Partition to be added.

    **Syntax:** `PARTITION ( partition_col_name  = partition_col_val [ , ... ] )`

#### DROP PARTITION

`ALTER TABLE DROP` statement drops the partition of the table.

##### Syntax

```sql
ALTER TABLE table_identifier DROP [ IF EXISTS ] partition_spec [PURGE]
```
     
##### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    Partition to be dropped.

    **Syntax:** `PARTITION ( partition_col_name  = partition_col_val [ , ... ] )`
     
### SET AND UNSET

#### SET TABLE PROPERTIES

`ALTER TABLE SET` command is used for setting the table properties. If a particular property was already set, 
this overrides the old value with the new one.

`ALTER TABLE UNSET` is used to drop the table property. 

##### Syntax

```sql
-- Set Table Properties 
ALTER TABLE table_identifier SET TBLPROPERTIES ( key1 = val1, key2 = val2, ... )

-- Unset Table Properties
ALTER TABLE table_identifier UNSET TBLPROPERTIES [ IF EXISTS ] ( key1, key2, ... )
```

#### SET SERDE

`ALTER TABLE SET` command is used for setting the SERDE or SERDE properties in Hive tables. If a particular property was already set, this overrides the old value with the new one.

##### Syntax

```sql
-- Set SERDE Properties
ALTER TABLE table_identifier [ partition_spec ]
    SET SERDEPROPERTIES ( key1 = val1, key2 = val2, ... )

ALTER TABLE table_identifier [ partition_spec ] SET SERDE serde_class_name
    [ WITH SERDEPROPERTIES ( key1 = val1, key2 = val2, ... ) ]
```

#### SET LOCATION And SET FILE FORMAT

`ALTER TABLE SET` command can also be used for changing the file location and file format for 
existing tables. 

##### Syntax

```sql
-- Changing File Format
ALTER TABLE table_identifier [ partition_spec ] SET FILEFORMAT file_format

-- Changing File Location
ALTER TABLE table_identifier [ partition_spec ] SET LOCATION 'new_location'
```

#### Parameters

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    Specifies the partition on which the property has to be set.

    **Syntax:** `PARTITION ( partition_col_name  = partition_col_val [ , ... ] )`

* **SERDEPROPERTIES ( key1 = val1, key2 = val2, ... )**

    Specifies the SERDE properties to be set.

### Examples

```sql
-- RENAME table 
DESC student;
+-----------------------+---------+-------+
|               col_name|data_type|comment|
+-----------------------+---------+-------+
|                   name|   string|   NULL|
|                 rollno|      int|   NULL|
|                    age|      int|   NULL|
|# Partition Information|         |       |
|             # col_name|data_type|comment|
|                    age|      int|   NULL|
+-----------------------+---------+-------+

ALTER TABLE Student RENAME TO StudentInfo;

-- After Renaming the table
DESC StudentInfo;
+-----------------------+---------+-------+
|               col_name|data_type|comment|
+-----------------------+---------+-------+
|                   name|   string|   NULL|
|                 rollno|      int|   NULL|
|                    age|      int|   NULL|
|# Partition Information|         |       |
|             # col_name|data_type|comment|
|                    age|      int|   NULL|
+-----------------------+---------+-------+

-- RENAME partition

SHOW PARTITIONS StudentInfo;
+---------+
|partition|
+---------+
|   age=10|
|   age=11|
|   age=12|
+---------+

ALTER TABLE default.StudentInfo PARTITION (age='10') RENAME TO PARTITION (age='15');

-- After renaming Partition
SHOW PARTITIONS StudentInfo;
+---------+
|partition|
+---------+
|   age=11|
|   age=12|
|   age=15|
+---------+

-- Add new columns to a table
DESC StudentInfo;
+-----------------------+---------+-------+
|               col_name|data_type|comment|
+-----------------------+---------+-------+
|                   name|   string|   NULL|
|                 rollno|      int|   NULL|
|                    age|      int|   NULL|
|# Partition Information|         |       |
|             # col_name|data_type|comment|
|                    age|      int|   NULL|
+-----------------------+---------+-------+

ALTER TABLE StudentInfo ADD columns (LastName string, DOB timestamp);

-- After Adding New columns to the table
DESC StudentInfo;
+-----------------------+---------+-------+
|               col_name|data_type|comment|
+-----------------------+---------+-------+
|                   name|   string|   NULL|
|                 rollno|      int|   NULL|
|               LastName|   string|   NULL|
|                    DOB|timestamp|   NULL|
|                    age|      int|   NULL|
|# Partition Information|         |       |
|             # col_name|data_type|comment|
|                    age|      int|   NULL|
+-----------------------+---------+-------+

-- Add a new partition to a table 
SHOW PARTITIONS StudentInfo;
+---------+
|partition|
+---------+
|   age=11|
|   age=12|
|   age=15|
+---------+

ALTER TABLE StudentInfo ADD IF NOT EXISTS PARTITION (age=18);

-- After adding a new partition to the table
SHOW PARTITIONS StudentInfo;
+---------+
|partition|
+---------+
|   age=11|
|   age=12|
|   age=15|
|   age=18|
+---------+

-- Drop a partition from the table 
SHOW PARTITIONS StudentInfo;
+---------+
|partition|
+---------+
|   age=11|
|   age=12|
|   age=15|
|   age=18|
+---------+

ALTER TABLE StudentInfo DROP IF EXISTS PARTITION (age=18);

-- After dropping the partition of the table
SHOW PARTITIONS StudentInfo;
+---------+
|partition|
+---------+
|   age=11|
|   age=12|
|   age=15|
+---------+

-- Adding multiple partitions to the table
SHOW PARTITIONS StudentInfo;
+---------+
|partition|
+---------+
|   age=11|
|   age=12|
|   age=15|
+---------+

ALTER TABLE StudentInfo ADD IF NOT EXISTS PARTITION (age=18) PARTITION (age=20);

-- After adding multiple partitions to the table
SHOW PARTITIONS StudentInfo;
+---------+
|partition|
+---------+
|   age=11|
|   age=12|
|   age=15|
|   age=18|
|   age=20|
+---------+

-- ALTER OR CHANGE COLUMNS
DESC StudentInfo;
+-----------------------+---------+-------+
|               col_name|data_type|comment|
+-----------------------+---------+-------+
|                   name|   string|   NULL|
|                 rollno|      int|   NULL|
|               LastName|   string|   NULL|
|                    DOB|timestamp|   NULL|
|                    age|      int|   NULL|
|# Partition Information|         |       |
|             # col_name|data_type|comment|
|                    age|      int|   NULL|
+-----------------------+---------+-------+

ALTER TABLE StudentInfo ALTER COLUMN name COMMENT "new comment";

--After ALTER or CHANGE COLUMNS
DESC StudentInfo;
+-----------------------+---------+-----------+
|               col_name|data_type|    comment|
+-----------------------+---------+-----------+
|                   name|   string|new comment|
|                 rollno|      int|       NULL|
|               LastName|   string|       NULL|
|                    DOB|timestamp|       NULL|
|                    age|      int|       NULL|
|# Partition Information|         |           |
|             # col_name|data_type|    comment|
|                    age|      int|       NULL|
+-----------------------+---------+-----------+

-- Change the fileformat
ALTER TABLE loc_orc SET fileformat orc;

ALTER TABLE p1 partition (month=2, day=2) SET fileformat parquet;

-- Change the file Location
ALTER TABLE dbx.tab1 PARTITION (a='1', b='2') SET LOCATION '/path/to/part/ways'

-- SET SERDE/ SERDE Properties
ALTER TABLE test_tab SET SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';

ALTER TABLE dbx.tab1 SET SERDE 'org.apache.hadoop' WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')

-- SET TABLE PROPERTIES
ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('winner' = 'loser');

-- SET TABLE COMMENT Using SET PROPERTIES
ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('comment' = 'A table comment.');

-- Alter TABLE COMMENT Using SET PROPERTIES
ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('comment' = 'This is a new comment.');

-- DROP TABLE PROPERTIES
ALTER TABLE dbx.tab1 UNSET TBLPROPERTIES ('winner');
```

### Related Statements

* [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
* [DROP TABLE](sql-ref-syntax-ddl-drop-table.html)
