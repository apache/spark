---
layout: global
title: ALTER VIEW
displayTitle: ALTER VIEW 
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
The `ALTER VIEW` statement changes various auxiliary properties of a view.


#### Rename view
Rename the existing view. If the view name already exists in the database, an exception is thrown. This operation does 
support moving the views cross databases. 
##### Syntax
{% highlight sql %}
ALTER VIEW [db_name.]view_name RENAME TO [db_name.]new_view_name
{% endhighlight %}


#### Set view properties
Set one or more properties of an existing view. The properties are the key value pairs. If the properties' keys exist, 
the values are replaced with the new values. If the properties' keys does not exist, the key value pairs are added into 
the properties.
##### Syntax
{% highlight sql %}
ALTER VIEW view_name SET TBLPROPERTIES (key1=val1, key2=val2, ...)
{% endhighlight %}


#### Drop view properties
Drop one or more properties of an existing view. If the specified keys do not exist, an exception is thrown. Use 
`IF EXISTS` to avoid the exception. 
##### Syntax
{% highlight sql %}
ALTER VIEW view_name UNSET TBLPROPERTIES [IF EXISTS] (key1=val1, key2=val2, ...)
{% endhighlight %}


#### Alter View As Select
`ALTER VIEW AS SELECT` statement changes the definition of a view, the `select_statement` must valid, and the `VIEW` 
must exist.
##### Syntax
{% highlight sql %}
ALTER VIEW view_name AS select_statement
{% endhighlight %}


#### Example
{% highlight sql %}
-- Rename only change the view name.
ALTER VIEW tempdb.view1 RENAME TO tempdb.view2
{% endhighlight %}

{% highlight sql %}
-- Use `DESC TABLE EXTENDED tempdb.view1` before and after the `ALTER VIEW` statement to verify the changes.
ALTER VIEW tempdb.view1 SET TBLPROPERTIES ('propKey1' = "propVal1", 'propKey2' = "propVal2" )
{% endhighlight %}

{% highlight sql %}
-- Use `DESC TABLE EXTENDED tempdb.view1` before and after the `ALTER VIEW` to verify the change.
ALTER VIEW tempdb.view1 UNSET TBLPROPERTIES ('propKey1'')
{% endhighlight %}

{% highlight sql %}
-- Do the select on tempdb.view1 before and after the `ALTER VIEW` statement to verify.
ALTER VIEW tempdb.view1 AS SELECT * FROM tempdb.view2
{% endhighlight %}

You can use [describe-table](sql-ref-syntax-aux-describe-table.html) command to verify the setting
##### Note:
`ALTER VIEW` statement does not support `SET SERDE` or `SET SERDEPROPERTIES` properties

