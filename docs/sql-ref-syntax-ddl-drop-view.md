---
layout: global
title: DROP VIEW
displayTitle: DROP VIEW 
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
`DROP VIEW` removes the metadata associated with a specified view from the catalog.

### Syntax
{% highlight sql %}
DROP VIEW [IF EXISTS] [database_name.]view_name
{% endhighlight %}

### Parameter
<dl>
  <dt><code><em>IF EXISTS</em></code></dt>
  <dd>
     If specified, no exception is thrown when the view does not exists.
  </dd>
  <dt><code><em>database_name</em></code></dt>
  <dd>
     Specifies the database name where view is present.
  </dd>
  <dt><code><em>view_name</em></code></dt>
  <dd>
     Specifies the view name to be dropped.
  </dd>
</dl>

### Example
{% highlight sql %}
-- Assumes a view named `employeeView` exists.
DROP VIEW employeeView;
+---------+--+
| Result  |
+---------+--+
+---------+--+

-- Assumes a view named `employeeView` exists in the `userdb` database
DROP VIEW userdb.employeeView;
+---------+--+
| Result  |
+---------+--+
+---------+--+

-- Assumes a view named `employeeView` does not exists.
-- Throws exception
DROP VIEW employeeView;
Error: org.apache.spark.sql.AnalysisException: Table or view not found: employeeView;
(state=,code=0)

-- Assumes a view named `employeeView` does not exists,Try with IF EXISTS
-- this time it will not throw exception
DROP VIEW IF EXISTS employeeView;
+---------+--+
| Result  |
+---------+--+
+---------+--+

{% endhighlight %}

### Related Statements
- [CREATE VIEW](sql-ref-syntax-ddl-create-view.html)
- [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
- [DROP DATABASE](sql-ref-syntax-ddl-drop-database.html)
