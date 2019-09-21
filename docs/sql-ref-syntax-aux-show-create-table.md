---
layout: global
title: SHOW CREATE TABLE
displayTitle: SHOW CREATE TABLE
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
`SHOW CREATE TABLE` returns the `CREATE TABLE` statement that creates a given table. `SHOW CREATE TABLE` on a non-existent table throws Exception.

### Syntax
{% highlight sql %}
SHOW CREATE TABLE table_name
{% endhighlight %}

### Parameters
<dl>
 <dt><code><em>table_name</em></code></dt>
 <dd>The name of the table to be used for SHOW CREATE TABLE.</dd>
</dl>

### Examples
{% highlight sql %}
CREATE TABLE test (c INT);

SHOW CREATE TABLE test;

-- the result of SHOW CREATE TABLE test
CREATE TABLE `test`(`c` INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  'transient_lastDdlTime' = '1569097524'
)
{% endhighlight %}

### Related Statements
 * [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
