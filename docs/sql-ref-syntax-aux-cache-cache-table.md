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
`CACHE TABLE` caches the table's contents in the RDD cache within memory or disk. This reduces scanning of the original files in future queries.

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
Only cache the table when it is first used, instead of immediately.

#### ***table_name***:
The name of an existing table.

#### ***table_property_list***:
Table property key value pairs.

#### ***query***:
A SELECT statement.
