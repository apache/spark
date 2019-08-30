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
`LOAD DATA` loads data from a directoy or a file into a table or into a partition in the table. A partition spec should be specified whenever the target table is partitioned. The `LOAD DATA` statement can only be used with tables created using the Hive format.

### Syntax
{% highlight sql %}
LOAD DATA [ LOCAL ] INPATH path [ OVERWRITE ] INTO TABLE table_name
  [ PARTITION ( partition_col_name [ = partition_col_val ] [ , ... ] ) ]
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>path</em></code></dt>
  <dd>Path of the file system.</dd>
</dl>

<dl>
  <dt><code><em>table_name</em></code></dt>
  <dd>The name of an existing table.</dd>
</dl>

<dl>
  <dt><code><em>PARTITION ( partition_col_name [ = partition_col_val ] [ , ... ] )</em></code></dt>
  <dd>Specifies one or more partition column and value pairs. The partition value is optional.</dd>
</dl>

<dl>
  <dt><code><em>LOCAL</em></code></dt>
  <dd>If specified, it causes the <code>INPATH</code> to be resolved against the local file system, instead of the default file system, which is typically a distributed storage.</dd>
</dl>

<dl>
  <dt><code><em>OVERWRITE</em></code></dt>
  <dd>By default, new data is appended to the table. If <code>OVERWRITE</code> is used, the table is instead overwritten with new data.</dd>
</dl>

### Examples
{% highlight sql %}
 -- Assuming the students table has already been created and populated.
 SELECT * FROM students;

     + -------------- + ------------------------------ + -------------- +
     | name           | address                        | student_id     |
     + -------------- + ------------------------------ + -------------- +
     | Amy Smith      | 123 Park Ave, San Jose         | 111111         |
     + -------------- + ------------------------------ + -------------- +

 CREATE TABLE test_load (name VARCHAR(64), address VARCHAR(64), student_id INT) USING HIVE;

 -- Assuming the students table is in '/user/hive/warehouse/'
 LOAD DATA LOCAL INPATH '/user/hive/warehouse/students' OVERWRITE INTO TABLE test_load;

 SELECT * FROM test_load;

     + -------------- + ------------------------------ + -------------- +
     | name           | address                        | student_id     |
     + -------------- + ------------------------------ + -------------- +
     | Amy Smith      | 123 Park Ave, San Jose         | 111111         |
     + -------------- + ------------------------------ + -------------- +
{% endhighlight %}

