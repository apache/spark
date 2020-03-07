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
`ALTER TABLE RENAME` statement changes the table name of an existing table in the database.

#### Syntax
{% highlight sql %}
ALTER TABLE table_identifier RENAME TO table_identifier

ALTER TABLE table_identifier partition_spec RENAME TO partition_spec

{% endhighlight %}

#### Parameters
<dl>
  <dt><code><em>table_identifier</em></code></dt>
  <dd>
    Specifies a table name, which may be optionally qualified with a database name.<br><br>
    <b>Syntax:</b>
      <code>
        [ database_name. ] table_name
      </code>
  </dd>
</dl>

<dl>
  <dt><code><em>partition_spec</em></code></dt>
  <dd>
    Partition to be renamed. <br><br>
    <b>Syntax:</b>
      <code>
        PARTITION ( partition_col_name  = partition_col_val [ , ... ] )
      </code>
  </dd>
</dl>


### ADD COLUMNS
`ALTER TABLE ADD COLUMNS` statement adds mentioned columns to an existing table.

#### Syntax
{% highlight sql %}
ALTER TABLE table_identifier ADD COLUMNS ( col_spec [ , col_spec ... ] )
{% endhighlight %}

#### Parameters
<dl>
  <dt><code><em>table_identifier</em></code></dt>
  <dd>
    Specifies a table name, which may be optionally qualified with a database name.<br><br>
    <b>Syntax:</b>
      <code>
        [ database_name. ] table_name
      </code>
  </dd>
</dl>

<dl>
  <dt><code><em>COLUMNS ( col_spec )</em></code></dt>
  <dd>Specifies the columns to be added to be renamed.</dd>
</dl>


### SET AND UNSET

#### SET TABLE PROPERTIES
`ALTER TABLE SET` command is used for setting the table properties. If a particular property was already set, 
this overrides the old value with the new one.

`ALTER TABLE UNSET` is used to drop the table property. 

##### Syntax
{% highlight sql %}

--Set Table Properties 
ALTER TABLE table_identifier SET TBLPROPERTIES ( key1 = val1, key2 = val2, ... )

--Unset Table Properties
ALTER TABLE table_identifier UNSET TBLPROPERTIES [ IF EXISTS ] ( key1, key2, ... )
  
{% endhighlight %}

#### SET SERDE
`ALTER TABLE SET` command is used for setting the SERDE or SERDE properties in Hive tables. If a particular property was already set,  
this overrides the old value with the new one.

##### Syntax
{% highlight sql %}

--Set SERDE Properties
ALTER TABLE table_identifier [ partition_spec ]
    SET SERDEPROPERTIES ( key1 = val1, key2 = val2, ... )

ALTER TABLE table_identifier [ partition_spec ] SET SERDE serde_class_name
    [ WITH SERDEPROPERTIES ( key1 = val1, key2 = val2, ... ) ]

{% endhighlight %}

#### SET LOCATION And SET FILE FORMAT
`ALTER TABLE SET` command can also be used for changing the file location and file format for 
existing tables. 

##### Syntax
{% highlight sql %}

--Changing File Format
ALTER TABLE table_identifier [ partition_spec ] SET FILEFORMAT file_format

--Changing File Location
ALTER TABLE table_identifier [ partition_spec ] SET LOCATION 'new_location'

{% endhighlight %}

#### Parameters
<dl>
  <dt><code><em>table_identifier</em></code></dt>
  <dd>
    Specifies a table name, which may be optionally qualified with a database name.<br><br>
    <b>Syntax:</b>
      <code>
        [ database_name. ] table_name
      </code>
  </dd>
</dl>

<dl>
  <dt><code><em>partition_spec</em></code></dt>
  <dd>
    Specifies the partition on which the property has to be set.<br><br>
    <b>Syntax:</b>
      <code>
        PARTITION ( partition_col_name  = partition_col_val [ , ... ] )
      </code>
  </dd>
</dl>

<dl>
  <dt><code><em>SERDEPROPERTIES ( key1 = val1, key2 = val2, ... )</em></code></dt>
  <dd>Specifies the SERDE properties to be set.</dd>
</dl>


### Examples
{% highlight sql %}

--RENAME table 
DESC student;
+--------------------------+------------+----------+--+
|         col_name         | data_type  | comment  |
+--------------------------+------------+----------+--+
| name                     | string     | NULL     |
| rollno                   | int        | NULL     |
| age                      | int        | NULL     |
| # Partition Information  |            |          |
| # col_name               | data_type  | comment  |
| age                      | int        | NULL     |
+--------------------------+------------+----------+--+

ALTER TABLE Student RENAME TO StudentInfo;

--After Renaming the table

DESC StudentInfo;
+--------------------------+------------+----------+--+
|         col_name         | data_type  | comment  |
+--------------------------+------------+----------+--+
| name                     | string     | NULL     |
| rollno                   | int        | NULL     |
| age                      | int        | NULL     |
| # Partition Information  |            |          |
| # col_name               | data_type  | comment  |
| age                      | int        | NULL     |
+--------------------------+------------+----------+--+

--RENAME partition

SHOW PARTITIONS StudentInfo;
+------------+--+
| partition  |
+------------+--+
| age=10     |
| age=11     |
| age=12     |
+------------+--+

ALTER TABLE default.StudentInfo PARTITION (age='10') RENAME TO PARTITION (age='15');

--After renaming Partition
SHOW PARTITIONS StudentInfo;
+------------+--+
| partition  |
+------------+--+
| age=11     |
| age=12     |
| age=15     |
+------------+--+

-- Add new column to a table

DESC StudentInfo;
+--------------------------+------------+----------+--+
|         col_name         | data_type  | comment  |
+--------------------------+------------+----------+--+
| name                     | string     | NULL     |
| rollno                   | int        | NULL     |
| age                      | int        | NULL     |
| # Partition Information  |            |          |
| # col_name               | data_type  | comment  |
| age                      | int        | NULL     |
+--------------------------+------------+----------+

ALTER TABLE StudentInfo ADD columns (LastName string, DOB timestamp);

--After Adding New columns to the table
DESC StudentInfo;
+--------------------------+------------+----------+--+
|         col_name         | data_type  | comment  |
+--------------------------+------------+----------+--+
| name                     | string     | NULL     |
| rollno                   | int        | NULL     |
| LastName                 | string     | NULL     |
| DOB                      | timestamp  | NULL     |
| age                      | int        | NULL     |
| # Partition Information  |            |          |
| # col_name               | data_type  | comment  |
| age                      | int        | NULL     |
+--------------------------+------------+----------+--+


--Change the fileformat
ALTER TABLE loc_orc SET fileformat orc;

ALTER TABLE p1 partition (month=2, day=2) SET fileformat parquet;

--Change the file Location
ALTER TABLE dbx.tab1 PARTITION (a='1', b='2') SET LOCATION '/path/to/part/ways'

-- SET SERDE/ SERDE Properties
ALTER TABLE test_tab SET SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';

ALTER TABLE dbx.tab1 SET SERDE 'org.apache.madoop' WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')

--SET TABLE PROPERTIES
ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('winner' = 'loser')

--DROP TABLE PROPERTIES
ALTER TABLE dbx.tab1 UNSET TBLPROPERTIES ('winner')

{% endhighlight %}


### Related Statements
- [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
- [DROP TABLE](sql-ref-syntax-ddl-drop-table.html)


