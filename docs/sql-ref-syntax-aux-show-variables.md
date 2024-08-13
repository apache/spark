---
layout: global
title: SHOW VARIABLES
displayTitle: SHOW VARIABLES
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

The `SHOW VARIABLES` statement is used to list all temporary variables 
which has been previously declared in the current session.

### Syntax

```sql
SHOW VARIABLES [ LIKE regex_pattern ]
```

### Parameters
* **regex_pattern**

  Specifies the regular expression pattern that is used to filter out unwanted variables.

  * Except for `*` and `|` character, the pattern works like a regular expression.
  * `*` alone matches 0 or more characters and `|` is used to separate multiple different regular expressions,
    any of which can match.
  * The leading and trailing blanks are trimmed in the input pattern before processing. The pattern match is case-insensitive.

### Examples
```sql
-- Declare variables.
DECLARE VARIABLE name1 INT DEFAULT 1;
DECLARE VARIABLE name2 INT DEFAULT 1;
DECLARE VARIABLE spark1 STRING DEFAULT 'spark1';
DECLARE VARIABLE spa_rk2 STRING DEFAULT 'spa_rk2';
DECLARE VARIABLE temp1 STRING DEFAULT 'temp1';
DECLARE VARIABLE tem2 STRING DEFAULT 'tem2';

-- List all variables
SHOW VARIABLES;
+----------+-----------+-------------------+---------+
|   name   | data_type | default_value_sql |  value  |
+----------+-----------+-------------------+---------+
|   name1  |    INT    |         1         |    1    |
|   name2  |    INT    |         1         |    1    |
|  spa_rk2 |   STRING  |     'spa_rk2'     | spa_rk2 |
|  spark1  |   STRING  |     'spark1'      | spark1  |
|   tem2   |   STRING  |      'tem2'       | tem2    |
|   temp1  |   STRING  |      'temp1'      | temp1   |
+----------+-----------+-------------------+---------+

-- List all variables matching the pattern `nam*`
SHOW VARIABLES LIKE 'nam*';
+----------+-----------+-------------------+---------+
|   name   | data_type | default_value_sql |  value  |
+----------+-----------+-------------------+---------+
|   name1  |    INT    |         1         |    1    |
|   name2  |    INT    |         1         |    1    |
+----------+-----------+-------------------+---------+

-- List all variables matching the pattern `nam|spark|temp*`
SHOW VARIABLES LIKE 'nam|spark|temp*';
+----------+-----------+-------------------+---------+
|   name   | data_type | default_value_sql |  value  |
+----------+-----------+-------------------+---------+
|   temp1  |   STRING  |      'temp1'      | temp1   |
+----------+-----------+-------------------+---------+
```

### Related statements
* [DECLARE VARIABLE](sql-ref-syntax-ddl-declare-variable.html)
* [DROP TEMPORARY VARIABLE](sql-ref-syntax-ddl-drop-variable.html)
* [SET VARIABLE](sql-ref-syntax-aux-set-var.html)
