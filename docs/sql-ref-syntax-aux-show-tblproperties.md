---
layout: global
title: SHOW TBLPROPERTIES
displayTitle: SHOW TBLPROPERTIES
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
This statement returns the value of a table property given an optional value for
a property key. If no key is specified then all the proerties are returned. 

### Syntax
{% highlight sql %}
SHOW TBLPROPERTIES tableIdentifier [(tablePropertyKey)]

tableIdentifier:= [db_name.]table_name
tablePropertyKey:= (unquoted_property_key | property_key_as_string_literal)
unquoted_property_key:= [key_part1][.key_part2][...]
{% endhighlight %}

**Note**
- Property value returned by this statement exludes some properties 
  that are internal to spark and hive. The excluded properties are :
  - All the properties that start with prefix `spark.sql`
  - Propery keys such as:  `EXTERNAL`, `comment`
  - All the properties generated intenally by hive to store statistics. Some of these
    properties are: `numFiles`, `numPartitions`, `numRows`.

### Examples
{% highlight sql %}
-- show all the user specified properties for table `customer`
SHOW TBLPROPERTIES customer;
-- show all the user specified properties for a qualified table `customer`
-- in database `salesdb`
SHOW TBLPROPERTIES salesdb.customer;
-- show value for unquoted propery key `show.me.the.value`
SHOW TBLPROPERTIES customer (show.me.the.value)
-- show value for property `show.me.the.value` specified as string literal
SHOW TBLPROPERTIES customer ('show.me.the.value')
{% endhighlight %}

### Related Statements
- [ALTER TABLE SET TBLPROPERTIES](sql-ref-syntax-ddl-alter-table.html)
- [SHOW TABLE](sql-ref-syntax-aux-show-table.html)
