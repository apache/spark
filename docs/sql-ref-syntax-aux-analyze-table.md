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

The `ANALYZE TABLE` statement collects statistics about the table to be used by the query optimizer to find a better query execution plan.

### Syntax
{% highlight sql %}
ANALYZE TABLE table_identifier [ partition_spec ]
    COMPUTE STATISTICS [ NOSCAN | FOR COLUMNS col [ , ... ] | FOR ALL COLUMNS ]

{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>table_identifier</em></code></dt>
  <dd>
    Specifies a table name, which may be optionally qualified with a database name.<br><br>
    <b>Syntax:</b>
      <code>
        [ database_name. ] table_name
      </code>
  </dd>
</dl>

<dl>
  <dt><code><em>partition_spec</em></code></dt>
  <dd>
    An optional parameter that specifies a comma separated list of key and value pairs
    for partitions. When specified, partition statistics is returned.<br><br>
    <b>Syntax:</b>
      <code>
        PARTITION ( partition_col_name [ = partition_col_val ] [ , ... ] )
      </code>
  </dd>
</dl>

<dl>
  <dt><code><em>[ NOSCAN | FOR COLUMNS col [ , ... ] | FOR ALL COLUMNS ]</em></code></dt>
    <dd>
      <ul>
        <li> If no analyze option is specified, <code>ANALYZE TABLE</code> collects the table's number of rows and size in bytes. </li>
        <li> <b>NOSCAN</b>
          <br> Collect only the table's size in bytes ( which does not require scanning the entire table ). </li>
        <li> <b>FOR COLUMNS col [ , ... ] <code> | </code> FOR ALL COLUMNS</b>
          <br> Collect column statistics for each column specified, or alternatively for every column, as well as table statistics.
        </li>
      </ul>
     </dd>
</dl>

### Examples
{% highlight sql %}
 ANALYZE TABLE students COMPUTE STATISTICS NOSCAN;

 DESC EXTENDED students;
     ......
     Statistics	2820 bytes
     ......

 ANALYZE TABLE students COMPUTE STATISTICS;

 DESC EXTENDED students;
     ......
     Statistics	2820 bytes, 3 rows
     ......

 ANALYZE TABLE students PARTITION (student_id = 111111) COMPUTE STATISTICS;

 DESC EXTENDED students PARTITION (student_id = 111111);
     ......
     Partition Statistics	919 bytes, 1 rows
     ......

 ANALYZE TABLE students COMPUTE STATISTICS FOR COLUMNS name;

 DESC EXTENDED students name;
     =default tbl=students
     col_name	name
     data_type	string
     comment	NULL
     min	NULL
     max	NULL
     num_nulls	0
     distinct_count	3
     avg_col_len	11
     max_col_len	13
     histogram	NULL

{% endhighlight %}