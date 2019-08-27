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
Creates a database with the given name. If database with the same name exists an exception will be thrown.

### Syntax
{% highlight sql %}
CREATE (DATABASE|SCHEMA) [IF NOT EXISTS] database_name
  [COMMENT database_comment]
  [LOCATION database_directory]
  [WITH DBPROPERTIES (property_name=property_value, ...)]

{% endhighlight %}

### Parameters
- ##### ***IF NOT EXISTS***:
Creates a database with the given name if it doesn't exists. If a database with the same name already exists, nothing will happen.
- ##### ***LOCATION***:
Path of the file system in which database to be created. If the specified path does not exist in the underlying file system, this command creates a directory with the path.
- ##### ***COMMENT***:
Description for the Database.
- ##### ***WITH DBPROPERTIES***:
Properties for the database in key-value pair.

### Examples
{% highlight sql %}
-- Create database `customer_db`. This throws exception if database with name customer_db already exists.
CREATE DATABASE customer_db;
-- Create database `customer_db` only if database with same name doesn't exist.
CREATE DATABASE IF NOT EXISTS customer_db;
-- Create database `customer_db` only if database with same name doesn't exist with `Comments`, `Specific Location` and `Database properties`.
CREATE DATABASE IF NOT EXISTS customer_db COMMENT 'This is customer database' LOCATION '/user' WITH DBPROPERTIES (ID=001, Name='John');
{% endhighlight %}


### Related statements.
- [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)

