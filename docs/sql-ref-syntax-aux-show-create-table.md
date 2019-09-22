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
`SHOW CREATE TABLE` returns the [CREATE TABLE statement](sql-ref-syntax-ddl-create-table.html) or [CREATE VIEW statement](sql-ref-syntax-ddl-create-view.html) that was used to create a given table or view. `SHOW CREATE TABLE` on a non-existent table or a temporary view throws Exception.

### Syntax
{% highlight sql %}
SHOW CREATE TABLE name
{% endhighlight %}

### Parameters
<dl>
 <dt><code><em>name</em></code></dt>
 <dd>The name of the table or view to be used for SHOW CREATE TABLE.</dd>
</dl>

### Examples
{% highlight sql %}
create table test_table (c INT) using json;

show create table test_table;

-- the result of SHOW CREATE TABLE test_table
CREATE TABLE `test_table` (`c` INT)
USING json

{% endhighlight %}

### Related Statements
 * [CREATE TABLE](sql-ref-syntax-ddl-create-table.html)
 * [CREATE VIEW](sql-ref-syntax-ddl-create-view.html)
