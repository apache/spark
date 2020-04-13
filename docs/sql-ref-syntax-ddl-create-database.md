---
layout: global
title: CREATE DATABASE
displayTitle: CREATE DATABASE 
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

Creates a database with the specified name. If database with the same name already exists, an exception will be thrown.

### Syntax

{% highlight sql %}
CREATE { DATABASE | SCHEMA } [ IF NOT EXISTS ] database_name
    [ COMMENT database_comment ]
    [ LOCATION database_directory ]
    [ WITH DBPROPERTIES ( property_name = property_value [ , ... ] ) ]
{% endhighlight %}

### Parameters

<dl>
    <dt><code><em>database_name</em></code></dt>
    <dd>Specifies the name of the database to be created.</dd>

    <dt><code><em>IF NOT EXISTS</em></code></dt>
    <dd>Creates a database with the given name if it doesn't exists. If a database with the same name already exists, nothing will happen.</dd>

    <dt><code><em>database_directory</em></code></dt>
    <dd>Path of the file system in which the specified database is to be created. If the specified path does not exist in the underlying file system, this command creates a directory with the path. If the location is not specified, the database will be created in the default warehouse directory, whose path is configured by the static configuration spark.sql.warehouse.dir.</dd>

    <dt><code><em>database_comment</em></code></dt>
    <dd>Specifies the description for the database.</dd>

    <dt><code><em>WITH DBPROPERTIES ( property_name=property_value [ , ... ] )</em></code></dt>
    <dd>Specifies the properties for the database in key-value pairs.</dd>
</dl>

### Examples

{% highlight sql %}
-- Create database `customer_db`. This throws exception if database with name customer_db
-- already exists.
CREATE DATABASE customer_db;

-- Create database `customer_db` only if database with same name doesn't exist.
CREATE DATABASE IF NOT EXISTS customer_db;

-- Create database `customer_db` only if database with same name doesn't exist with 
-- `Comments`,`Specific Location` and `Database properties`.
CREATE DATABASE IF NOT EXISTS customer_db COMMENT 'This is customer database' LOCATION '/user'
    WITH DBPROPERTIES (ID=001, Name='John');

-- Verify that properties are set.
DESCRIBE DATABASE EXTENDED customer_db;
   +-------------------------+--------------------------+
   |database_description_item|database_description_value|
   +-------------------------+--------------------------+
   |            Database Name|               customer_db|
   |              Description| This is customer database|
   |                 Location|     hdfs://hacluster/user|
   |               Properties|   ((ID,001), (Name,John))|
   +-------------------------+--------------------------+
{% endhighlight %}

### Related Statements

 * [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
 * [DROP DATABASE](sql-ref-syntax-ddl-drop-database.html)
