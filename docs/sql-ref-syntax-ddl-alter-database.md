---
layout: global
title: ALTER DATABASE
displayTitle: ALTER DATABASE
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
You can author metadata associated with a database by setting `DBPROPERTIES`. Using this command 
you can set key-value pairs in the `DBPROPERTIES`. The specified property values override any existing
value with the same property name. Please note that the usage of `SCHEMA` and `DATABASE` are interchangable 
and mean the same thing. An error message is issued if the database does not exist in the system. This
command is mostly used to record the metadata for a database and can be used for auditing purposes.

### Syntax
{% highlight sql %}
ALTER {DATABASE | SCHEMA} database_name SET DBPROPERTIES (propery_name=property_value, ...)
{% endhighlight %}

### Example
{% highlight sql %}
ALTER DATABASE inventory SET DBPROPERTIES ('Edited-by' = 'John', 'Edit-date' = '01/01/2001')
{% endhighlight %}

You can use [describe-database](sql-ref-syntax-aux-describe-database.html) command to verify the setting
of properties.

