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
Given number of functions supported by Spark is quite large, this statement
in conjuction with [describe function](sql-ref-syntax-aux-describe-function.html)
may be used to quickly find the function and understand its usage. The `LIKE` 
clause is optional and supported only for compatibility with other systems.

### Syntax
{% highlight sql %}
SHOW [ function_kind ] FUNCTIONS ( [ LIKE ] function_name | regex_pattern )
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>function_kind</em></code></dt>
  <dd>
    Specifies the name space of the function to be searched upon. The valid name spaces are :
    <ul>
      <li><b>USER</b> - Looks up the function(s) among the user defined functions.</li>
      <li><b>SYSTEM</b> - Looks up the function(s) among the system defined functions.</li>
      <li><b>ALL</b> -  Looks up the function(s) among both user and system defined functions.</li>
    </ul>
  </dd>
  <dt><code><em>function_name</em></code></dt>
  <dd>
    Specifies a name of an existing function in the system. The function name may be
    optionally qualified with a database name. If `function_name` is qualified with
    a database then the function is resolved from the user specified database, otherwise
    it is resolved from the current database.<br><br>
    <b>Syntax:</b>
      <code>
        [database_name.]function_name
      </code>
  </dd>
  <dt><code><em>regex_pattern</em></code></dt>
  <dd>
    Specifies a regular expression pattern that is used to limit the results of the
    statement.
    <ul>
      <li>Only `*` and `|` are allowed as wildcard pattern.</li>
      <li>Excluding `*` and `|` the remaining pattern follows the regex semantics.</li>
      <li>The leading and trailing blanks are trimmed in the input pattern before processing.</li> 
    </ul>
  </dd>
</dl>

### Examples
{% highlight sql %}
-- List a system function `trim` by searching both user defined and system
-- defined functions.
SHOW FUNCTIONS trim;
  +--------+
  |function|
  +--------+
  |trim    |
  +--------+

-- List a system function `concat` by searching system defined functions.
SHOW SYSTEM FUNCTIONS concat;
  +--------+
  |function|
  +--------+
  |concat  |
  +--------+

-- List a qualified function `max` from database `salesdb`. 
SHOW SYSTEM FUNCTIONS salesdb.max;
  +--------+
  |function|
  +--------+
  |max     |
  +--------+

-- List all functions starting with `t`
SHOW FUNCTIONS LIKE 't*';
  +-----------------+
  |function         |
  +-----------------+
  |tan              |
  |tanh             |
  |timestamp        |
  |tinyint          |
  |to_csv           |
  |to_date          |
  |to_json          |
  |to_timestamp     |
  |to_unix_timestamp|
  |to_utc_timestamp |
  |transform        |
  |transform_keys   |
  |transform_values |
  |translate        |
  |trim             |
  |trunc            |
  +-----------------+

-- List all functions starting with `yea` or `windo`
SHOW FUNCTIONS LIKE 'yea*|windo*';
  +--------+
  |function|
  +--------+
  |window  |
  |year    |
  +--------+

-- Use normal regex pattern to list function names that has 4 characters
-- with `t` as the starting character.
SHOW FUNCTIONS LIKE 't[a-z][a-z][a-z]';
  +--------+
  |function|
  +--------+
  |tanh    |
  |trim    |
  +--------+
{% endhighlight %}

### Related statements
- [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
