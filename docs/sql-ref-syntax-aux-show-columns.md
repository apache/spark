---
layout: global
title: SHOW COLUMNS
displayTitle: SHOW COLUMNS
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
Return the list of columns in a table. If the table does not exist, an exception is thrown.

### Syntax
{% highlight sql %}
SHOW COLUMNS {IN | FROM} [db.]table {IN | FROM} database
{% endhighlight %}
**Note**
- You can list the columns of a table in a database other than current database in one of following
ways:
  - By qualifying the table name with a database name other than current database.
  - By specifying the database name in the database parameter.
- Keywords `IN` and `FROM` are interchangeable.

### Examples
{% highlight sql %}
-- List the columns of table employee in current database.
SHOW COLUMNS IN employee
-- List the columns of table employee in salesdb database.
SHOW COLUMNS IN salesdb.employee
-- List the columns of table employee in salesdb database
SHOW COLUMNS IN employee IN salesdb
{% endhighlight %}
