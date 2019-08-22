---
layout: global
title: DROP VIEW
displayTitle: DROP VIEW 
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
Removes the view, which was created by CREATE VIEW statement. DROP VIEW only involves
changes in metadata in the metastore database.

### Syntax
{% highlight sql %}
DROP VIEW [IF EXISTS] [dbname.]viewName
{% endhighlight %}

### Parameter

**dbname**

*Database* name where view present, if not provided it takes the *current Database*.

**viewName**

*View* to be dropped

**IF EXISTS**

If specified will not throw exception if *view* is not present.

### Example
{% highlight sql %}
DROP VIEW USERDB.USERVIEW
DROP TABLE USERVIEW
DROP VIEW IF EXISTS USERDB.USERVIEW
DROP VIEW IF EXISTS USERVIEW
{% endhighlight %}

