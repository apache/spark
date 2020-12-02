---
layout: global
title: Scalar User Defined Functions (UDFs)
displayTitle: Scalar User Defined Functions (UDFs)
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

User-Defined Functions (UDFs) are user-programmable routines that act on one row. This documentation lists the classes that are required for creating and registering UDFs. It also contains examples that demonstrate how to define and register UDFs and invoke them in Spark SQL.

### UserDefinedFunction

To define the properties of a user-defined function, the user can use some of the methods defined in this class.

* **asNonNullable(): UserDefinedFunction**

    Updates UserDefinedFunction to non-nullable.

* **asNondeterministic(): UserDefinedFunction**

    Updates UserDefinedFunction to nondeterministic.

* **withName(name: String): UserDefinedFunction**

    Updates UserDefinedFunction with a given name.

### Examples

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% include_example udf_scalar scala/org/apache/spark/examples/sql/UserDefinedScalar.scala%}
</div>
<div data-lang="java"  markdown="1">
  {% include_example udf_scalar java/org/apache/spark/examples/sql/JavaUserDefinedScalar.java%}
</div>
</div>

### Related Statements
* [User Defined Aggregate Functions (UDAFs)](sql-ref-functions-udf-aggregate.html)
* [Integration with Hive UDFs/UDAFs/UDTFs](sql-ref-functions-udf-hive.html)
