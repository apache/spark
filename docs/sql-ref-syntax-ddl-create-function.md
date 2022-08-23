---
layout: global
title: CREATE FUNCTION
displayTitle: CREATE FUNCTION
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

The `CREATE FUNCTION` statement is used to create a temporary or permanent function
in Spark. Temporary functions are scoped at a session level where as permanent
functions are created in the persistent catalog and are made available to
all sessions. The resources specified in the `USING` clause are made available
to all executors when they are executed for the first time. In addition to the
SQL interface, spark allows users to create custom user defined scalar and
aggregate functions using Scala, Python and Java APIs. Please refer to 
[Scalar UDFs](sql-ref-functions-udf-scalar.html) and
[UDAFs](sql-ref-functions-udf-aggregate.html) for more information.

### Syntax

```sql
CREATE [ OR REPLACE ] [ TEMPORARY ] FUNCTION [ IF NOT EXISTS ]
    function_name AS class_name [ resource_locations ]
```

### Parameters

* **OR REPLACE**

    If specified, the resources for the function are reloaded. This is mainly useful
    to pick up any changes made to the implementation of the function. This
    parameter is mutually exclusive to `IF NOT EXISTS` and can not
    be specified together.

* **TEMPORARY**

    Indicates the scope of function being created. When `TEMPORARY` is specified, the
    created function is valid and visible in the current session. No persistent
    entry is made in the catalog for these kind of functions.

* **IF NOT EXISTS**

    If specified, creates the function only when it does not exist. The creation
    of function succeeds (no error is thrown) if the specified function already
    exists in the system. This parameter is mutually exclusive to `OR REPLACE`
    and can not be specified together.

* **function_name**

    Specifies a name of function to be created. The function name may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] function_name`

* **class_name**

    Specifies the name of the class that provides the implementation for function to be created.
    The implementing class should extend one of the base classes as follows:

    * Should extend `UDF` or `UDAF` in `org.apache.hadoop.hive.ql.exec` package.
    * Should extend `AbstractGenericUDAFResolver`, `GenericUDF`, or
      `GenericUDTF` in `org.apache.hadoop.hive.ql.udf.generic` package.
    * Should extend `UserDefinedAggregateFunction` in `org.apache.spark.sql.expressions` package.

* **resource_locations**

    Specifies the list of resources that contain the implementation of the function
    along with its dependencies.

    **Syntax:** `USING { { (JAR | FILE | ARCHIVE) resource_uri } , ... }`

### Examples

```sql
-- 1. Create a simple UDF `SimpleUdf` that increments the supplied integral value by 10.
--    import org.apache.hadoop.hive.ql.exec.UDF;
--    public class SimpleUdf extends UDF {
--      public int evaluate(int value) {
--        return value + 10;
--      }
--    }
-- 2. Compile and place it in a JAR file called `SimpleUdf.jar` in /tmp.

-- Create a table called `test` and insert two rows.
CREATE TABLE test(c1 INT);
INSERT INTO test VALUES (1), (2);

-- Create a permanent function called `simple_udf`. 
CREATE FUNCTION simple_udf AS 'SimpleUdf'
    USING JAR '/tmp/SimpleUdf.jar';

-- Verify that the function is in the registry.
SHOW USER FUNCTIONS;
+------------------+
|          function|
+------------------+
|default.simple_udf|
+------------------+

-- Invoke the function. Every selected value should be incremented by 10.
SELECT simple_udf(c1) AS function_return_value FROM test;
+---------------------+
|function_return_value|
+---------------------+
|                   11|
|                   12|
+---------------------+

-- Created a temporary function.
CREATE TEMPORARY FUNCTION simple_temp_udf AS 'SimpleUdf' 
    USING JAR '/tmp/SimpleUdf.jar';

-- Verify that the newly created temporary function is in the registry.
-- Please note that the temporary function does not have a qualified
-- database associated with it.
SHOW USER FUNCTIONS;
+------------------+
|          function|
+------------------+
|default.simple_udf|
|   simple_temp_udf|
+------------------+

-- 1. Modify `SimpleUdf`'s implementation to add supplied integral value by 20.
--    import org.apache.hadoop.hive.ql.exec.UDF;
  
--    public class SimpleUdfR extends UDF {
--      public int evaluate(int value) {
--        return value + 20;
--      }
--    }
-- 2. Compile and place it in a jar file called `SimpleUdfR.jar` in /tmp.

-- Replace the implementation of `simple_udf`
CREATE OR REPLACE FUNCTION simple_udf AS 'SimpleUdfR'
    USING JAR '/tmp/SimpleUdfR.jar';

-- Invoke the function. Every selected value should be incremented by 20.
SELECT simple_udf(c1) AS function_return_value FROM test;
+---------------------+
|function_return_value|
+---------------------+
|                   21|
|                   22|
+---------------------+
```

### Related Statements

* [SHOW FUNCTIONS](sql-ref-syntax-aux-show-functions.html)
* [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
* [DROP FUNCTION](sql-ref-syntax-ddl-drop-function.html)
