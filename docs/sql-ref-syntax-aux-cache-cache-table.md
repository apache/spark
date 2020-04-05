---
layout: global
title: CACHE TABLE
displayTitle: CACHE TABLE
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
`CACHE TABLE` statement caches contents of a table or output of a query with the given storage level. If a query is cached, then a temp view will be created for this query.
This reduces scanning of the original files in future queries. 

### Syntax
{% highlight sql %}
CACHE [ LAZY ] TABLE table_identifier
    [ OPTIONS ( 'storageLevel' [ = ] value ) ] [ [ AS ] query ]
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>LAZY</em></code></dt>
  <dd>Only cache the table when it is first used, instead of immediately.</dd>
</dl>

<dl>
  <dt><code><em>table_identifier</em></code></dt>
  <dd>
    Specifies the table or view name to be cached. The table or view name may be optionally qualified with a database name.<br><br>
    <b>Syntax:</b>
      <code>
        [ database_name. ] table_name
      </code>
  </dd>
</dl>

<dl>
  <dt><code><em>OPTIONS ( 'storageLevel' [ = ] value )</em></code></dt>
  <dd>
  <code>OPTIONS</code> clause with <code>storageLevel</code> key and value pair. A Warning is issued when a key other than <code>storageLevel</code> is used. The valid options for <code>storageLevel</code> are:
    <ul>
      <li><code>NONE</code></li>
      <li><code>DISK_ONLY</code></li>
      <li><code>DISK_ONLY_2</code></li>
      <li><code>MEMORY_ONLY</code></li>
      <li><code>MEMORY_ONLY_2</code></li>
      <li><code>MEMORY_ONLY_SER</code></li>
      <li><code>MEMORY_ONLY_SER_2</code></li>
      <li><code>MEMORY_AND_DISK</code></li>
      <li><code>MEMORY_AND_DISK_2</code></li>
      <li><code>MEMORY_AND_DISK_SER</code></li>
      <li><code>MEMORY_AND_DISK_SER_2</code></li>
      <li><code>OFF_HEAP</code></li>
    </ul>
    An Exception is thrown when an invalid value is set for <code>storageLevel</code>. If <code>storageLevel</code> is not explicitly set using <code>OPTIONS</code> clause, the default <code>storageLevel</code> is set to <code>MEMORY_AND_DISK</code>.
  </dd>
</dl>

<dl>
  <dt><code><em>query</em></code></dt>
  <dd>A query that produces the rows to be cached. It can be in one of following formats:
    <ul>
      <li>a <code>SELECT</code> statement</li>
      <li>a <code>TABLE</code> statement</li>
      <li>a <code>FROM</code> statement</li>
    </ul>
   </dd>
</dl>

### Examples
{% highlight sql %}
CACHE TABLE testCache OPTIONS ('storageLevel' 'DISK_ONLY') SELECT * FROM testData;
{% endhighlight %}

### Related Statements
  * [CLEAR CACHE](sql-ref-syntax-aux-cache-clear-cache.html)
  * [UNCACHE TABLE](sql-ref-syntax-aux-cache-uncache-table.html)
  * [REFRESH TABLE](sql-ref-syntax-aux-refresh-table.html)
  * [REFRESH](sql-ref-syntax-aux-cache-refresh.html)

