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

    Specifies the name of an existing function. Whether `function_name` refers to a temporary
    function or a persistent function is selected by the `TEMPORARY` keyword, not by the
    identifier &mdash; without `TEMPORARY` the name always targets a persistent function (even if
    you write `session.f` or `system.session.f`, in which case Spark looks for a persistent
    function in a schema literally named `session`).

    * With `TEMPORARY`: the name may be optionally qualified with the session schema
      (`session` or `system.session`); for example, `DROP TEMPORARY FUNCTION f`,
      `DROP TEMPORARY FUNCTION session.f`, and `DROP TEMPORARY FUNCTION system.session.f` all
      drop the same temporary function.

      **Syntax:** `[ { session | system.session } . ] function_name`

    * Without `TEMPORARY`: the name may be optionally qualified with a database name (or a catalog
      and database) and resolves to a persistent function in that schema.

      **Syntax:** `[ catalog_name. ] [ database_name. ] function_name`

    The built-in namespace `system.builtin` cannot be dropped: `DROP FUNCTION system.builtin.abs`
    raises `FORBIDDEN_OPERATION`.

* **TEMPORARY**

    Required to drop a temporary function. Without `TEMPORARY`, `DROP FUNCTION` only considers
    persistent functions.

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
