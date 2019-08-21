---
layout: global
title: ANALYZE TABLE
displayTitle: ANALYZE TABLE
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

ANALYZE TABLE statement can be used to collect statistics about the table. The information can be used by the query optimizer to find a better plan.

### Syntax
{% highlight sql %}
ANALYZE TABLE [db_name.]table_name [PARTITION partition_spec] COMPUTE STATISTICS
  [analyze_option]

partition_spec:
    : (part_col_name1[=val1], part_col_name2[=val2], ...)

analyze_option
    : NOSCAN | FOR COLUMNS col1 [, col2, ...] | FOR ALL COLUMNS

{% endhighlight %}

### Example
{% highlight sql %}
ANALYZE TABLE table1 COMPUTE STATISTICS FOR COLUMNS id, value

ANALYZE TABLE db.table1 PARTITION(ds='2008-04-09') COMPUTE STATISTICS NOSCAN
{% endhighlight %}

### Parameters

#### ***table_name***:
The name of an existing table.

#### ***partition_spec***:
Partition column specification.

#### ***analyze_option***:
If no analyze option is specified, ANALYZE TABLE statement collect only basic statistics for the table (number of rows, size in bytes).

- NOSCAN

  Collect only statistics that do not require scanning the whole table (that is, size in bytes).

- FOR COLUMNS col1 [, col2, ...] `|` FOR ALL COLUMNS

  Collect column statistics for the specified columns or all columns in addition to table statistics.
