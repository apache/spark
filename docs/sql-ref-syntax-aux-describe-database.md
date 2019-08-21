---
layout: global
title: DESCRIBE DATABASE
displayTitle: DESCRIBE DATABASE
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

**Syntax**
{% highlight sql %}
DESCRIBE DATABASE [EXTENDED] db_name 
{% endhighlight %}

**Example**
{% highlight sql %}
DESCRIBE DATABASE EXTENDED tempdb
{% endhighlight %}



Return the metadata of an existing database(name, comment and location). If the database does not exist,
 
an exception is thrown.

When `extended` is specified, it also shows the database's properties.

You can use the abbreviation `DESC` for the `DESCRIBE` statement.

  

