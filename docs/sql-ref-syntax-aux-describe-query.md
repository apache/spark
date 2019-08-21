---
layout: global
title: DESCRIBE QUERY
displayTitle: DESCRIBE QUERY
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
The `DESCRIBE QUERY` statement is used to return the metadata of output
of a query. 

### Syntax
{% highlight sql %}
{DESC | DESCRIBE} [QUERY] query 

query:= {cte | select-query | inline-table | TABLE table-name}
{% endhighlight %}
**Note**
* For detailed syntax of query parameter please refer to [select-statement](sql-ref-syntax-qry-select.html)
  and the examples below.
* `DESC` and `DESCRIBE` are interchangeble and mean the same thing.

### Examples
{% highlight sql %}
-- Returns column name, column type and comment info for a simple select query
DESCRIBE QUERY select * customer;
-- Returns column name, column type and comment info for a inline table.
DESC QUERY VALUES(1.00D, 'hello') as test_tab(c1, c2);
-- Returns column name, column type and comment info for query with set operations.
DESC QUERY SELECT int_col FROM int_tab UNION ALL select double_col from double_tab
-- Returns column name, column type and comment info for CTE.
DESCRIBE QUERY WITH cte AS (SELECT cust_id from customer) SELECT * FROM cte;
{% endhighlight %}

### Related Statements
- [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
- [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
- [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
