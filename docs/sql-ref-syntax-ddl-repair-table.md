---
layout: global
title: REPAIR TABLE
displayTitle: REPAIR TABLE
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
`MSCK REPAIR TABLE` recovers all the partitions in the directory of a table and updates the catalog. `MSCK REPAIR TABLE` on a non-existent table or a table without partitions throws Exception. Another way to recover partitions is to use `ALTER TABLE RECOVER PARTITIONS`.

### Syntax
{% highlight sql %}
MSCK REPAIR TABLE table_name
{% endhighlight %}

### Examples
{% highlight sql %}
MSCK REPAIR TABLE t1;

{% endhighlight %}
### Related Statements
 * [ALTER TABLE](sql-ref-syntax-ddl-alter-table.html)
