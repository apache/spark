---
layout: global
title: DESCRIBE FUNCTION
displayTitle: DESCRIBE FUNCTION
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

Return the metadata such as qualified function name, implementing class and usage of an existing function.
Input parameter function name may be optionally qualified. If the function is not qualified then the function is
resolved from the current database. If EXTENDED option is specified then additionally extended usage information
including usage examples are returned. 

### Syntax
{% highlight sql %}
{DESC | DESCRIBE} FUNCTION [EXTENDED] [db_name.]function_name
{% endhighlight %}

### Examples
{% highlight sql %}
-- Describe a builtin scalar function.
-- Returns function name, implementing class and usage
DESC FUNCTION Trim;
-- Describe a builtin scalar function.
-- Returns function name, implementing class and usage and examples.
DESC FUNCTION EXTENDED trim;
-- Describe a builtin aggregate function
DESC FUNCTION Max;
-- Describe a builtin user defined aggregate function
-- Returns function name, implementing class and usage and examples.
DESC FUNCTION EXTENDED Explode
-- Create a UDF
CREATE FUNCTION myFunc
AS 'com.mycompany.functions.myFunc'
USING JAR 'jar-file-uri'
-- describe the user defined function
DESC FUNCTION myFunc
{% endhighlight %}

### Related Statements
- [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
- [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
- [DESCRIBE QUERY](sql-ref-syntax-aux-describe-query.html)
