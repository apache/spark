---
layout: global
title: SHOW FUNCTIONS
displayTitle: SHOW FUNCTIONS
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
Returns the list of functions after applying an optional regex pattern.
Three optional parameters SYSTEM, USER, ALL may be specified to list
system functions, user defined function or both user and system defined
funtions respectively. Given Spark supports hundreds of functions, this
statement in conjuction with [describe function](sql-ref-syntax-aux-describe-function.html)
may be used to quickly find the function and understand its usage.

### Syntax
{% highlight sql %}
SHOW [USER|SYSTEM|ALL] FUNCTIONS ([LIKE] qualified_function_name | regex_pattern)
qualified_function_name:= [db.]function_name
{% endhighlight %}

### Parameters
- ##### ***qualified_function_name***:
  Function name may be optionally qualified with database.
- ##### ***regex_pattern***:
  A regex pattern that is used as a filtering out unwanted functions.
  - Only '*' and '|' are allowed as wildcard pattern.
  - Excluding '*' and '|' the remaining pattern follows the regex semantics.
  - The leading and trailing blanks are trimmed in the input pattern before processing.  

### Examples
{% highlight sql %}
-- List a system function `trim` by searching both user defined and system defined functions.
SHOW FUNCTIONS trim;
-- List a system function `trim` by searching system defined functions.
SHOW SYSTEM FUNCTIONS trim;
-- List a system function `trim` by searching user defined functions.
SHOW SYSTEM FUNCTIONS my_udf;
-- List a qualified funtion. among system defined functions.
SHOW SYSTEM FUNCTIONS sales_db.trim;
-- List all functions starting with `t`
SHOW FUNCTIONS LIKE `t*`;
-- List all functions starting with `t` or `a`
SHOW FUNCTIONS LIKE `t* | a*`;
-- Use normal regex pattern to list function names that has 4 characters
-- with `t` as the starting character.
SHOW functions like 't[a-z][a-z][a-z]';
{% endhighlight %}

### Related statements.
- [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
