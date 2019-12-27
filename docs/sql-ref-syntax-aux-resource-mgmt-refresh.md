---
layout: global
title: REFRESH
displayTitle: REFRESH
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
`REFRESH` is used to invalidate and refresh all the cached data (and the associated metadata) for
all Dataset, that contains the given data source path. Path matching is by prefix, i.e. "/" would 
invalidate everything that is cached.

### Syntax
{% highlight sql %}
REFRESH resource_name
{% endhighlight %}

### Parameters
<dl>
 <dt><code><em>resource_name</em></code></dt>
 <dd>The path of the resource that is to be refreshed.</dd>
</dl>

### Examples
{% highlight sql %}
REFRESH /tmp/test.jar;
REFRESH "/tmp/test.txt";
REFRESH '/tmp/test.txt';
{% endhighlight %}

### Related Statements
 * [LIST JAR](sql-ref-syntax-aux-resource-mgmt-list-jar.html)
 * [ADD FILE](sql-ref-syntax-aux-resource-mgmt-add-file.html)
 * [LIST FILE](sql-ref-syntax-aux-resource-mgmt-list-file.html)
 * [ADD JAR](sql-ref-syntax-aux-resource-mgmt-add-jar.html)
