---
layout: global
title: Functions
displayTitle: Functions
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

Spark SQL provides two function features to meet a wide range of user needs: built-in functions and user-defined functions (UDFs).
Built-in functions are commonly used routines that Spark SQL predefines and a complete list of the functions can be found in the [Built-in Functions](api/sql/index.html) API document. UDFs allow users to define their own functions when the system’s built-in functions are not enough to perform the desired task.

### Built-in Functions

Spark SQL has some categories of frequently-used built-in functions for aggregation, arrays/maps, date/timestamp, and JSON data.
This subsection presents the usages and descriptions of these functions.

#### Scalar Functions
 * [Array Functions](sql-ref-functions-builtin.html#array-functions)
 * [Collection Functions](sql-ref-functions-builtin.html#collection-functions)
 * [Struct Functions](sql-ref-functions-builtin.html#struct-functions)
 * [Map Functions](sql-ref-functions-builtin.html#map-functions)
 * [Date and Timestamp Functions](sql-ref-functions-builtin.html#date-and-timestamp-functions)
 * [Mathematical Functions](sql-ref-functions-builtin.html#mathematical-functions)
 * [String Functions](sql-ref-functions-builtin.html#string-functions)
 * [Bitwise Functions](sql-ref-functions-builtin.html#bitwise-functions)
 * [Conversion Functions](sql-ref-functions-builtin.html#conversion-functions)
 * [Conditional Functions](sql-ref-functions-builtin.html#conditional-functions)
 * [Predicate Functions](sql-ref-functions-builtin.html#predicate-functions)
 * [Hash Functions](sql-ref-functions-builtin.html#hash-functions)
 * [CSV Functions](sql-ref-functions-builtin.html#csv-functions)
 * [JSON Functions](sql-ref-functions-builtin.html#json-functions)
 * [XML Functions](sql-ref-functions-builtin.html#xml-functions)
 * [URL Functions](sql-ref-functions-builtin.html#url-functions)
 * [Misc Functions](sql-ref-functions-builtin.html#misc-functions)

#### Aggregate-like Functions
 * [Aggregate Functions](sql-ref-functions-builtin.html#aggregate-functions)
 * [Window Functions](sql-ref-functions-builtin.html#window-functions)

#### Generator Functions
* [Generator Functions](sql-ref-functions-builtin.html#generator-functions)

### UDFs (User-Defined Functions)

User-Defined Functions (UDFs) are a feature of Spark SQL that allows users to define their own functions when the system's built-in functions are not enough to perform the desired task. To use UDFs in Spark SQL, users must first define the function, then register the function with Spark, and finally call the registered function. The User-Defined Functions can act on a single row or act on multiple rows at once. Spark SQL also supports integration of existing Hive implementations of UDFs, UDAFs and UDTFs.

 * [Scalar User-Defined Functions (UDFs)](sql-ref-functions-udf-scalar.html)
 * [User-Defined Aggregate Functions (UDAFs)](sql-ref-functions-udf-aggregate.html)
 * [Integration with Hive UDFs/UDAFs/UDTFs](sql-ref-functions-udf-hive.html)
