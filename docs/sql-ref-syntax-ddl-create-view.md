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
A view is a virtual table based on the result set of base query. The base query
can involve joins, expressions, reordered columns, column aliases, and other SQL
features that can make a query hard to understand or maintain.
A view is purely a logical construct (an alias for a query) with no physical
data behind it.

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

### Examples
{% highlight sql %}
-- Create a global temporary view `v1` if it does not exist.
CREATE GLOBAL TEMPORARY VIEW IF NOT EXISTS v1 AS SELECT * FROM t1;
-- Create or replace view `v2` with comments and metadata.
CREATE OR REPLACE VIEW v2 
    (c1 COMMENT 'comment for c1', c2) 
    COMMENT 'comment for v1' 
    TBLPROPERTIES (key1=value1, key2=value2) 
    AS SELECT * FROM t1;
{% endhighlight %}

### Parameters
#### OR REPLACE
If a view of same name already exists, it will be replaced.
#### [GLOBAL] TEMPORARY
TEMPORARY views are session-scoped and will be dropped when session ends 
because it skips persisting the definition in the underlying metastore, if any.
GLOBAL TEMPORARY views are tied to a system preserved temporary database `global_temp`.
#### IF NOT EXISTS
Creates a view if it does not exists.
#### COMMENT
Table-level and column-level comments can be specified in CREATE VIEW statement.
#### TBLPROPERTIES
Metadata key-value pairs.
#### AS SELECT
A [SELECT](sql-ref-syntax-qry-select.md) statement that constructs the view from base tables or other views.

### Related Statements
- [ALTER VIEW](sql-ref-syntax-ddl-alter-view.md)
- [DROP VIEW](sql-ref-syntax-ddl-drop-view.md)