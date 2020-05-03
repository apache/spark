---
layout: global
title: DROP DATABASE
displayTitle: DROP DATABASE
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

Drop a database and delete the directory associated with the database from the file system. An 
exception will be thrown if the database does not exist in the system. 

### Syntax

{% highlight sql %}
DROP { DATABASE | SCHEMA } [ IF EXISTS ] dbname [ RESTRICT | CASCADE ]
{% endhighlight %}

### Parameters

<dl>
  <dt><code><em>DATABASE | SCHEMA</em></code></dt>
  <dd><code>DATABASE</code> and <code>SCHEMA</code> mean the same thing, either of them can be used.</dd>
</dl>

<dl>
  <dt><code><em>IF EXISTS</em></code></dt>
  <dd>If specified, no exception is thrown when the database does not exist.</dd>
</dl>

<dl>
  <dt><code><em>RESTRICT</em></code></dt>
  <dd>If specified, will restrict dropping a non-empty database and is enabled by default.</dd>
</dl>

<dl>
  <dt><code><em>CASCADE</em></code></dt>
  <dd>If specified, will drop all the associated tables and functions.</dd>
</dl>

### Examples

{% highlight sql %}
-- Create `inventory_db` Database
CREATE DATABASE inventory_db COMMENT 'This database is used to maintain Inventory';

-- Drop the database and it's tables
DROP DATABASE inventory_db CASCADE;

-- Drop the database using IF EXISTS
DROP DATABASE IF EXISTS inventory_db CASCADE;
{% endhighlight %}

### Related Statements

 * [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
 * [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
 * [SHOW DATABASES](sql-ref-syntax-aux-show-databases.html)
