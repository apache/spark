---
layout: global
title: DROP TEMPORARY VARIABLE
displayTitle: DROP TEMPORARY VARIABLE 
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

The `DROP TEMPORARY VARIABLE` statement drops a temporary variable. An exception will
be thrown if the variable does not exist. 

### Syntax

```sql
DROP TEMPORARY { VAR | VARIABLE } [ IF EXISTS ] variable_name
```

### Parameters

* **variable_name**

    Specifies the name of an existing variable. The function name may be
    optionally qualified with a `system.session` or `session`.

    **Syntax:** `[ system. [ session.] ] variable_name`

* **IF EXISTS**

    If specified, no exception is thrown when the variable does not exist.

### Examples

```sql
-- Create a temporary variable var1
DECLARE VARIABLE var1 INT;

-- Drop temporary variable
DROP TEMPORARY VARIABLE var1;

-- Try to drop temporary variable which is not present
DROP TEMPORARY VARIABLE var1;
[VARIABLE_NOT_FOUND] The variable `system`.`session`.`var1` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a schema and catalog, verify the current_schema() output, or qualify the name with the correct schema and catalog.
To tolerate the error on drop use DROP VARIABLE IF EXISTS. SQLSTATE: 42883
  
-- Drop temporary variable if it exists
DROP TEMPORARY VARIABLE IF EXISTS var1;
```

### Related Statements

* [DECLARE VARIABLE](sql-ref-syntax-ddl-declare-variable.html)
