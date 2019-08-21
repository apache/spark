---
layout: global
title: DROP TABLE
displayTitle: DROP TABLE
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
Drop a table and delete the directory associated with the table from the file system if this is not an
EXTERNAL table. If the table to drop does not exist, an exception is thrown.

### Syntax
{% highlight sql %}
DROP TABLE [IF EXISTS] [database_name.]table_name
{% endhighlight %}

 ### Example
{% highlight sql %}
DROP TABLE USERDB.TABLE1
DROP TABLE TABLE1
DROP TABLE IF EXISTS USERDB.TABLE1
DROP TABLE IF EXISTS TABLE1
{% endhighlight %}

