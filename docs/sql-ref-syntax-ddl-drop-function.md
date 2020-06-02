---
layout: global
title: DROP FUNCTION
displayTitle: DROP FUNCTION 
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

The `DROP FUNCTION` statement drops a temporary or user defined function (UDF). An exception will
be thrown if the function does not exist. 

### Syntax

```sql
DROP [ TEMPORARY ] FUNCTION [ IF EXISTS ] function_name
```

### Parameters

* **function_name**

    Specifies the name of an existing function. The function name may be
    optionally qualified with a database name.

    **Syntax:** `[ database_name. ] function_name`

* **TEMPORARY**

    Should be used to delete the `TEMPORARY` function.

* **IF EXISTS**

    If specified, no exception is thrown when the function does not exist.

### Examples

```sql
-- Create a permanent function `test_avg`
CREATE FUNCTION test_avg AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage';

-- List user functions
SHOW USER FUNCTIONS;
+----------------+
|        function|
+----------------+
|default.test_avg|
+----------------+

-- Create Temporary function `test_avg`
CREATE TEMPORARY FUNCTION test_avg AS
    'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage';

-- List user functions
SHOW USER FUNCTIONS;
+----------------+
|        function|
+----------------+
|default.test_avg|
|        test_avg|
+----------------+

-- Drop Permanent function
DROP FUNCTION test_avg;

-- Try to drop Permanent function which is not present
DROP FUNCTION test_avg;
Error: Error running query:
org.apache.spark.sql.catalyst.analysis.NoSuchPermanentFunctionException:
Function 'default.test_avg' not found in database 'default'; (state=,code=0)

-- List the functions after dropping, it should list only temporary function
SHOW USER FUNCTIONS;
+--------+
|function|
+--------+
|test_avg|
+--------+
  
-- Drop Temporary function
DROP TEMPORARY FUNCTION IF EXISTS test_avg;
```

### Related Statements

* [CREATE FUNCTION](sql-ref-syntax-ddl-create-function.html)
* [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
* [SHOW FUNCTION](sql-ref-syntax-aux-show-functions.html)
