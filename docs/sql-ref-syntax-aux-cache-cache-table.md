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
CACHE TABLE statement can be used to cache the contents of the table in memory using the RDD cache. This enables subsequent queries to avoid scanning the original files as much as possible.

### Syntax
{% highlight sql %}
CACHE [LAZY] TABLE [db_name.]table_name
  [OPTIONS (table_property_list)] [[AS] query]

table_property_list:
    : (table_property_key1 [[=]table_property_value1], table_property_key2 [[=]table_property_value2], ...)

{% endhighlight %}

### Example
{% highlight sql %}
CACHE TABLE testCache OPTIONS ('storageLevel' 'DISK_ONLY') SELECT * FROM testData
{% endhighlight %}

### Parameters

#### ***LAZY***:
Cache the table lazily instead of eagerly scanning the entire table.

#### ***table_name***:
The name of an existing table.

#### ***table_property_list***:
Table property key value pairs.

#### ***query***:
A SELECT statement.
