---
layout: global
title: CREATE VIEW
displayTitle: CREATE VIEW 
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
Views are based on result-set of complex base `SQL` queries that are hard to
maintain or understand. `CREATE VIEW` constructs a virtual table that has no
physical data therefore other operations like `ALTER VIEW` and `DROP VIEW` 
only change metadata. 

### Syntax
{% highlight sql %}
CREATE [OR REPLACE] [[GLOBAL] TEMPORARY] VIEW [IF NOT EXISTS] [db_name.]view_name
    [(column_name [COMMENT column_comment], ...) ]
    create_view_clauses
    AS SELECT ...;
    
    create_view_clauses (order insensitive):
        [COMMENT view_comment]
        [TBLPROPERTIES (property_name = property_value, ...)]
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>OR REPLACE</em></code></dt>
  <dd>If a view of same name already exists, it will be replaced.</dd>
</dl>
<dl>
  <dt><code><em>[GLOBAL] TEMPORARY</em></code></dt>
  <dd>TEMPORARY views are session-scoped and will be dropped when session ends 
      because it skips persisting the definition in the underlying metastore, if any.
      GLOBAL TEMPORARY views are tied to a system preserved temporary database `global_temp`.</dd>
</dl>
<dl>
  <dt><code><em>IF NOT EXISTS</em></code></dt>
  <dd>Creates a view if it does not exists.</dd>
</dl>
<dl>
  <dt><code><em>COMMENT</em></code></dt>
  <dd>View-level and column-level comments can be specified in CREATE VIEW statement.</dd>
</dl>
<dl>
  <dt><code><em>TBLPROPERTIES</em></code></dt>
  <dd>Metadata key-value pairs.</dd>
</dl>
<dl>
  <dt><code><em>AS SELECT</em></code></dt>
  <dd>A <a href="sql-ref-syntax-qry-select.md">SELECT</a> statement that constructs the view from base tables or other views.</dd>
</dl>

### Examples
{% highlight sql %}
-- Create a global temporary view `subscribed_movies` if it does not exist.
CREATE GLOBAL TEMPORARY VIEW IF NOT EXISTS subscribed_movies 
    AS SELECT mo.member_id, mb.full_name, mo.movie_title
        FROM movies AS mo INNER JOIN members AS mb 
        ON mo.member_id = mb.id;

-- Create or replace view for `experienced_employee` with comments.
CREATE OR REPLACE VIEW experienced_employee
    (ID COMMENT 'Unique identification number', Name) 
    COMMENT 'View for experienced employees'
    AS SELECT id, name FROM all_employee
        WHERE working_years > 5;

{% endhighlight %}

### Related Statements
- [ALTER VIEW](sql-ref-syntax-ddl-alter-view.md)
- [DROP VIEW](sql-ref-syntax-ddl-drop-view.md)