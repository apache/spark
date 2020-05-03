---
layout: global
title: DROP TABLE
displayTitle: DROP TABLE
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

`DROP TABLE` deletes the table and removes the directory associated with the table from the file system
if the table is not `EXTERNAL` table. If the table is not present it throws an exception.

In case of an external table, only the associated metadata information is removed from the metastore database.

### Syntax

{% highlight sql %}
DROP TABLE [ IF EXISTS ] table_identifier
{% endhighlight %}

### Parameter

<dl>
  <dt><code><em>IF EXISTS</em></code></dt>
  <dd>
     If specified, no exception is thrown when the table does not exists.
  </dd>
  <dt><code><em>table_identifier</em></code></dt>
  <dd>
    Specifies the table name to be dropped. The table name may be optionally qualified with a database name.<br><br>
    <b>Syntax:</b>
      <code>
        [ database_name. ] table_name
      </code>
  </dd>
</dl>

### Examples

{% highlight sql %}
-- Assumes a table named `employeetable` exists.
DROP TABLE employeetable;

-- Assumes a table named `employeetable` exists in the `userdb` database
DROP TABLE userdb.employeetable;

-- Assumes a table named `employeetable` does not exists.
-- Throws exception
DROP TABLE employeetable;
  Error: org.apache.spark.sql.AnalysisException: Table or view not found: employeetable;
  (state=,code=0)

-- Assumes a table named `employeetable` does not exists,Try with IF EXISTS
-- this time it will not throw exception
DROP TABLE IF EXISTS employeetable;
{% endhighlight %}

### Related Statements

 * [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
 * [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
 * [DROP DATABASE](sql-ref-syntax-ddl-drop-database.html)
