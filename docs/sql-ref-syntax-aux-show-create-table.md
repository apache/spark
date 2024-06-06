---
layout: global
title: SHOW CREATE TABLE
displayTitle: SHOW CREATE TABLE
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

`SHOW CREATE TABLE` returns the [CREATE TABLE statement](sql-ref-syntax-ddl-create-table.html) or [CREATE VIEW statement](sql-ref-syntax-ddl-create-view.html) that was used to create a given table or view. `SHOW CREATE TABLE` on a non-existent table or a temporary view throws an exception.

### Syntax

```sql
SHOW CREATE TABLE table_identifier [ AS SERDE ]
```

### Parameters

* **table_identifier**

    Specifies a table or view name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **AS SERDE**

    Generates Hive DDL for a Hive SerDe table.

### Examples

```sql
CREATE TABLE test (c INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    TBLPROPERTIES ('prop1' = 'value1', 'prop2' = 'value2');

SHOW CREATE TABLE test;
+----------------------------------------------------+
|                                      createtab_stmt|
+----------------------------------------------------+
|CREATE TABLE `default`.`test` (`c` INT)
 USING text
 TBLPROPERTIES (
   'transient_lastDdlTime' = '1586269021',
   'prop1' = 'value1',
   'prop2' = 'value2')
+----------------------------------------------------+

SHOW CREATE TABLE test AS SERDE;
+------------------------------------------------------------------------------+
|                                                                createtab_stmt|
+------------------------------------------------------------------------------+
|CREATE TABLE `default`.`test`(
  `c` INT)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
 WITH SERDEPROPERTIES (
   'serialization.format' = ',',
   'field.delim' = ',')
 STORED AS
   INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
   OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
 TBLPROPERTIES (
   'prop1' = 'value1',
   'prop2' = 'value2',
   'transient_lastDdlTime' = '1641800515')
+------------------------------------------------------------------------------+
```

### Related Statements

* [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
* [CREATE VIEW](sql-ref-syntax-ddl-create-view.html)
