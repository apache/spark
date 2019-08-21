---
layout: global
title: LOAD DATA
displayTitle: LOAD DATA
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
The LOAD DATA statement can be used to load data from a file into a table or a partition in the table. The target table must not be temporary. A partition spec must be provided if and only if the target table is partitioned. The LOAD DATA statement is only supported for tables created using the Hive format.

### Syntax
{% highlight sql %}
LOAD DATA [LOCAL] INPATH path [OVERWRITE] INTO TABLE [db_name.]table_name
  [PARTITION partition_spec]

partition_spec:
    : (part_col_name1=val1, part_col_name2=val2, ...)
{% endhighlight %}

### Example
{% highlight sql %}
LOAD DATA LOCAL INPATH 'data/files/f1.txt'
  OVERWRITE INTO TABLE testDB.testTable PARTITION (p1 = 3, p2 = 4)
{% endhighlight %}

### Parameters

#### ***path***:
Path of the file system.

#### ***table_name***:
The name of an existing table.

#### ***partition_spec***:
One or more partition column name and value pairs.

##### ***LOCAL***:
If specified, local file system is used. Otherwise, the default file system is used.

##### ***OVERWRITE***:
If specified, existing data in the table is overwritten. Otherwise, new data is appended to the table.
