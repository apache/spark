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
ALTER TABLE statement changes the schema or properties of a table.

### Syntax
{% highlight sql %}
--Rename a table
ALTER TABLE [old_db_name.]old_table_name RENAME TO [new_db_name.]new_table_name
ALTER TABLE table PARTITION spec1 RENAME TO PARTITION spec2

--Add columns to an existing table
ALTER TABLE table_name ADD COLUMNS (col_spec[, col_spec ...])

--Drop columns of an existing table
ALTER TABLE table_name DROP [COLUMN] column_name

--Add or drop partition
ALTER TABLE table_name { ADD [IF NOT EXISTS] | DROP [IF EXISTS] } PARTITION (partition_spec) [PURGE]

--Change a column definition of an existing table
ALTER TABLE table [PARTITION partition_spec] CHANGE [COLUMN] column_old_name column_new_name column_dataType [COMMENT column_comment] [FIRST | AFTER column_name];

ALTER TABLE table_name REPLACE COLUMNS (col_spec[, col_spec ...])

--RECOVER PARTITIONS clause scans the table data directory to find any new partition directories
ALTER TABLE table_name RECOVER PARTITIONS

--Drop one or more properties of an existing table
ALTER (TABLE) table_name UNSET TBLPROPERTIES [IF EXISTS] (key1, key2, ...)

--Set the SerDe or the SerDe properties of a table or partition
ALTER TABLE table_name [PARTITION spec] SET SERDE serde_name [WITH SERDEPROPERTIES props];
ALTER TABLE table_name [PARTITION spec] SET SERDEPROPERTIES serde_properties;

--Set the properties of an existing table or view. If a particular property was already set, this overrides the old value with the new one.
ALTER [TABLE] table_name [PARTITION (partition_spec)] SET TBLPROPERTIES (key1=val1, key2=val2, ...)

--To change the file format of data to be in, for a table or partition:
ALTER TABLE table_name [PARTITION (partition_spec)]
  SET { FILEFORMAT file_format
  | LOCATION 'hdfs_path_of_directory' }

new_name ::= [new_database.]new_table_name

col_spec ::= col_name type_name  COMMENT 'column-comment' [kudu_attributes]

kudu_attributes ::= { [NOT] NULL | ENCODING codec | COMPRESSION algorithm |
  DEFAULT constant | BLOCK_SIZE number }

partition_spec ::= partition_col=constant_value

table_properties ::= 'name'='value'[, 'name'='value' ...]

serde_properties ::= 'name'='value'[, 'name'='value' ...]

file_format :: like { PARQUET | TEXTFILE | RCFILE | SEQUENCEFILE | AVRO }
{% endhighlight %}


### Examples
{% highlight sql %}
-- add new column to a table
ALTER TABLE t1 ADD columns (s string, t timestamp);
ALTER TABLE srcpart ADD partition (ds='2008-04-08', hr='11'); 

--Drop a column
ALTER TABLE p2 DROP column x;
ALTER TABLE tab1 DROP PARTITION (a='300')

--change the fileformat
ALTER TABLE loc_orc SET fileformat orc;
ALTER TABLE p1 partition (month=2, day=2) SET fileformat parquet;

-- set Properties
ALTER TABLE test_tab SET SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';
ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('winner' = 'loser')
ALTER TABLE dbx.tab1 PARTITION (a='1', b='2') SET LOCATION '/path/to/part/ways'
ALTER TABLE dbx.tab1 SET SERDE 'org.apache.madoop' WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')

--drop property
ALTER TABLE dbx.tab1 UNSET TBLPROPERTIES ('winner')

--rename partition
ALTER TABLE dbx.tab1 PARTITION (a='1', b='q') RENAME TO PARTITION (a='100', b='p')

--change Column
ALTER TABLE dbx.tab1 CHANGE COLUMN col1 col1 INT COMMENT 'this is col1'

{% endhighlight %}


### Related Statements
- [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
- [DROP TABLE](sql-ref-syntax-ddl-drop-table.html)


