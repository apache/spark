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

### Example
{% highlight sql %}
CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM t1;
CREATE VIEW v1 (c1 COMMENT 'comment for c1', c2) COMMENT 'comment for v1' TBLPROPERTIES (key1=value1, key2=value2) AS SELECT * FROM t1;
{% endhighlight %}