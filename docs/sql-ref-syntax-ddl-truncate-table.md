---
layout: global
title: TRUNCATE TABLE
displayTitle: TRUNCATE TABLE
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
TRUNCATE TABLE statement removes all the rows from a table or partition(s). The table must not be a 
temporary table, an external table, or a view. User can specify partial partition_spec for 
truncating multiple partitions at once, omitting partition_spec will truncate all partitions in the table.

### Syntax
{% highlight sql %}
TRUNCATE TABLE table_name [PARTITION partition_spec];
 
partition_spec:
  : (partition_column = partition_col_value, partition_column = partition_col_value, ...)
{% endhighlight %}


### Examples
{% highlight sql %}
-- Removes all rows from the table in the partion specified
TRUNCATE TABLE partition_date2_1 partition(dt=date '2000-01-01', region=2);

-- Removes all rows from the table from all partitions
TRUNCATE TABLE num_result;

{% endhighlight %}


### Related Statements
- [DROP TABLE](sql-ref-syntax-ddl-drop-table.html)
- [ALTER TABLE](sql-ref-syntax-ddl-alter-tabley.html)

