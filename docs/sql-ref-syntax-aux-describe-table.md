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
{% highlight sql %}
{DESC | DESCRIBE} [TABLE] [format] table_identifier [partition_spec] [col_name]
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>format</em></code></dt>
  <dd>
    Specifies the optional format of describe output. If `EXTENDED` is specified
    then additional metadata information (such as parent database, owner, and access time)
    is returned. 
  </dd>
  <dt><code><em>table_identifier</em></code></dt>
  <dd>
    Specifies a table name, which may be optionally qualified with a database name.<br><br>
    <b>Syntax:</b>
      <code>
        [database_name.]table_name
      </code>
  </dd>
  <dt><code><em>partition_spec</em></code></dt>
  <dd>
    An optional parameter that specifies a comma separated list of key and value pairs
    for paritions. When specified, additional partition metadata is returned.<br><br>
    <b>Syntax:</b>
      <code>
        PARTITION (partition_col_name  = partition_col_val [ , ... ])
      </code>
  </dd>  
  <dt><code><em>col_name</em></code></dt>
  <dd>
    An optional paramter that specifies the column name that needs to be described.
    The supplied column name may be optionally qualified. Parameters `partition_spec`
    and `col_name` are  mutually exclusive and can not be specified together. Currently
    nested columns are not allowed to be specified.<br><br>
    
    <b>Syntax:</b>
      <code>
        [database_name.][table_name.]column_name
      </code>
   </dd>
</dl>

### Examples
{% highlight sql %}
-- Creates a table `customer`. Assumes current database is `salesdb`.
CREATE TABLE customer(
    cust_id INT,
    state VARCHAR(20),
    name STRING COMMENT 'Short name'
  )
  USING parquet
  PARTITION BY state;
  ;

-- Returns basic metadata information for unqualified table `customer`
DESCRIBE TABLE customer;
  +-----------------------+---------+----------+
  |col_name               |data_type|comment   |
  +-----------------------+---------+----------+
  |cust_id                |int      |null      |
  |name                   |string   |Short name|
  |state                  |string   |null      |
  |# Partition Information|         |          |
  |# col_name             |data_type|comment   |
  |state                  |string   |null      |
  +-----------------------+---------+----------+

-- Returns basic metadata information for qualified table `customer`
DESCRIBE TABLE salesdb.customer;
  +-----------------------+---------+----------+
  |col_name               |data_type|comment   |
  +-----------------------+---------+----------+
  |cust_id                |int      |null      |
  |name                   |string   |Short name|
  |state                  |string   |null      |
  |# Partition Information|         |          |
  |# col_name             |data_type|comment   |
  |state                  |string   |null      |
  +-----------------------+---------+----------+

-- Returns additional metadata such as parent database, owner, access time etc.
DESCRIBE TABLE EXTENDED customer;
  +----------------------------+------------------------------+----------+
  |col_name                    |data_type                     |comment   |
  +----------------------------+------------------------------+----------+
  |cust_id                     |int                           |null      |
  |name                        |string                        |Short name|
  |state                       |string                        |null      |
  |# Partition Information     |                              |          |
  |# col_name                  |data_type                     |comment   |
  |state                       |string                        |null      |
  |                            |                              |          |
  |# Detailed Table Information|                              |          |
  |Database                    |salesdb                       |          |
  |Table                       |customer                      |          |
  |Owner                       |<table owner>                 |          |
  |Created Time                |Fri Aug 30 09:26:04 PDT 2019  |          |
  |Last Access                 |Wed Dec 31 16:00:00 PST 1969  |          |
  |Created By                  |<spark version>               |          |
  |Type                        |MANAGED                       |          |
  |Provider                    |parquet                       |          |
  |Location                    |file:.../salesdb.db/customer  |          |
  |Serde Library               |...serde.ParquetHiveSerDe     |          |
  |InputFormat                 |...MapredParquetInputFormat   |          |
  |OutputFormat                |...MapredParquetOutputFormat  |          |
  +----------------------------+------------------------------+----------+

-- Returns partition metadata such as partitioning column name, column type and comment.
DESCRIBE TABLE customer PARTITION (state = 'AR');

  +--------------------------------+-----------------------------------------+----------+
  |col_name                        |data_type                                |comment   |
  +--------------------------------+-----------------------------------------+----------+
  |cust_id                         |int                                      |null      |
  |name                            |string                                   |Short name|
  |state                           |string                                   |null      |
  |# Partition Information         |                                         |          |
  |# col_name                      |data_type                                |comment   |
  |state                           |string                                   |null      |
  |                                |                                         |          |
  |# Detailed Partition Information|                                         |          |
  |Database                        |salesdb                                  |          |
  |Table                           |customer                                 |          |
  |Partition Values                |[state=AR]                               |          |
  |Location                        |file:.../salesdb.db/customer/state=AR    |          |
  |Serde Library                   |...serde.ParquetHiveSerDe                |          |
  |InputFormat                     |...parquet.MapredParquetInputFormat      |          |
  |OutputFormat                    |...parquet.MapredParquetOutputFormat     |          |
  |Storage Properties              |[path=file:.../salesdb.db/customer,      |          |
  |                                | serialization.format=1]                 |          |
  |Partition Parameters            |{rawDataSize=-1, numFiles=1l,            |          |
  |                                | transient_lastDdlTime=1567185245,       |          |
  |                                | totalSize=688,                          |          |
  |                                | COLUMN_STATS_ACCURATE=false, numRows=-1}|          |
  |Created Time                    |Fri Aug 30 10:14:05 PDT 2019             |          |
  |Last Access                     |Wed Dec 31 16:00:00 PST 1969             |          |
  |Partition Statistics            |688 bytes                                |          |
  +--------------------------------+-----------------------------------------+----------+

-- Returns the metadata for `name` column.
-- Optional `TABLE` clause is omitted and column is fully qualified.
DESCRIBE customer salesdb.customer.name;
  +---------+----------+
  |info_name|info_value|
  +---------+----------+
  |col_name |name      |
  |data_type|string    |
  |comment  |Short name|
  +---------+----------+
{% endhighlight %}

### Related Statements
- [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
- [DESCRIBE QUERY](sql-ref-syntax-aux-describe-query.html)
- [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
