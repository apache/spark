---
layout: global
title: SHOW TABLES
displayTitle: SHOW TABLES
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

List all the `tables` from the database with `database-name` and `is temporary table`.

### Syntax
{% highlight sql %}
SHOW TABLES [{FROM|IN} database_name] [LIKE 'regex_pattern']
{% endhighlight %}

### Parameter
<dl>
  <dt><code><em>{FROM|IN} database_name</em></code></dt>
  <dd>
     Listing the tables from this `database`.
  </dd>
  <dt><code><em>LIKE 'regex_pattern'</em></code></dt>
  <dd>
     A regex pattern that is used to filter out unwanted functions.
       <br> - Only `*` and `|` are allowed as wildcard pattern.
       <br> - Excluding `*` and `|` the remaining pattern follows the regex semantics.
       <br> - The leading and trailing blanks are trimmed in the input pattern before processing.  
  </dd>
</dl>

### Example
{% highlight sql %}
-- List all tables in default database
SHOW TABLES;
+-----------+------------+--------------+--+
| database  | tableName  | isTemporary  |
+-----------+------------+--------------+--+
| default   | sam        | false        |
| default   | sam1       | false        |
| default   | suj        | false        |
+-----------+------------+--------------+--+

-- List all tables from userdb database 
SHOW TABLES FROM userdb;
+-----------+------------+--------------+--+
| database  | tableName  | isTemporary  |
+-----------+------------+--------------+--+
| userdb    | user1      | false        |
| userdb    | user2      | false        |
+-----------+------------+--------------+--+

-- List all tables in userdb database
SHOW TABLES IN userdb;
+-----------+------------+--------------+--+
| database  | tableName  | isTemporary  |
+-----------+------------+--------------+--+
| userdb    | user1      | false        |
| userdb    | user2      | false        |
+-----------+------------+--------------+--+

-- List all tables from default database matching the pattern `sam*`
SHOW TABLES FROM default LIKE 'sam*';
+-----------+------------+--------------+--+
| database  | tableName  | isTemporary  |
+-----------+------------+--------------+--+
| default   | sam        | false        |
| default   | sam1       | false        |
+-----------+------------+--------------+--+

{% endhighlight %}

### Related statements.
- [CREATE TABLE ](sql-ref-syntax-ddl-create-table.html)
- [DROP TABLE ](ssql-ref-syntax-ddl-drop-table.html)
- [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
- [DROP DATABASE](sql-ref-syntax-ddl-drop-database.html)