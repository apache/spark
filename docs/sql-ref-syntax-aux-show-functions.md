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
in conjunction with [describe function](sql-ref-syntax-aux-describe-function.html)
may be used to quickly find the function and understand its usage. The `LIKE` 
clause is optional and supported only for compatibility with other systems.

### Syntax

```sql
SHOW [ function_kind ] FUNCTIONS [ [ LIKE ] { function_name | regex_pattern } ]
```

### Parameters

* **function_kind**

    Specifies the name space of the function to be searched upon. The valid name spaces are :

    * **USER** - Looks up the function(s) among the user defined functions.
    * **SYSTEM** - Looks up the function(s) among the system defined functions.
    * **ALL** -  Looks up the function(s) among both user and system defined functions.

* **function_name**

    Specifies a name of an existing function in the system. The function name may be
    optionally qualified with a database name. If `function_name` is qualified with
    a database then the function is resolved from the user specified database, otherwise
    it is resolved from the current database.

    **Syntax:** `[ database_name. ] function_name`

* **regex_pattern**

    Specifies a regular expression pattern that is used to filter the results of the
    statement.

    * Except for `*` and `|` character, the pattern works like a regular expression.
    * `*` alone matches 0 or more characters and `|` is used to separate multiple different regular expressions,
       any of which can match.
    * The leading and trailing blanks are trimmed in the input pattern before processing. The pattern match is case-insensitive.

### Examples

```sql
-- List a system function `trim` by searching both user defined and system
-- defined functions.
SHOW FUNCTIONS trim;
+--------+
|function|
+--------+
|    trim|
+--------+

-- List a system function `concat` by searching system defined functions.
SHOW SYSTEM FUNCTIONS concat;
+--------+
|function|
+--------+
|  concat|
+--------+

-- List a qualified function `max` from database `salesdb`. 
SHOW SYSTEM FUNCTIONS salesdb.max;
+--------+
|function|
+--------+
|     max|
+--------+

-- List all functions starting with `t`
SHOW FUNCTIONS LIKE 't*';
+-----------------+
|         function|
+-----------------+
|              tan|
|             tanh|
|        timestamp|
|          tinyint|
|           to_csv|
|          to_date|
|          to_json|
|     to_timestamp|
|to_unix_timestamp|
| to_utc_timestamp|
|        transform|
|   transform_keys|
| transform_values|
|        translate|
|             trim|
|            trunc|
|           typeof|
+-----------------+

-- List all functions starting with `yea` or `windo`
SHOW FUNCTIONS LIKE 'yea*|windo*';
+--------+
|function|
+--------+
|  window|
|    year|
+--------+

-- Use normal regex pattern to list function names that has 4 characters
-- with `t` as the starting character.
SHOW FUNCTIONS LIKE 't[a-z][a-z][a-z]';
+--------+
|function|
+--------+
|    tanh|
|    trim|
+--------+
```

### Related Statements

* [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
