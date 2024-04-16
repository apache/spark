---
layout: global
title: Function Invocation
displayTitle: Function Invocation
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

A function invocation executes a builtin function or a user-defined function after associating arguments to the function’s parameters.

Spark supports positional parameter invocation as well as named parameter invocation.

#### Positional parameter invocation

Each argument is assigned to the matching parameter at the position it is specified.

This notation can be used by all functions unless it is explicitly documented that named parameter invocation is required.

If the function supports optional parameters, trailing parameters for which no arguments have been specified, are defaulted.

#### Named parameter invocation

Arguments are explicitly assigned to parameters using the parameter names published by the function.

This notation must be used for a select subset of built-in functions which allow numerous optional parameters, making positional parameter invocation impractical.
These functions may allow a mixed invocation where a leading set of parameters are expected to be assigned by position and the trailing, optional set of parameters by name.

### Syntax

```sql
function_name ( [ argExpr | table_argument ] [, ...]
                [ namedParameter => [ argExpr | table_argument ] [, ...] )

table_argument
  { TABLE ( { table_name | query } )
    [ table_partition ]
    [ table_order ]

table_partitioning
  { WITH SINGLE PARTITION |
    { PARTITION | DISTRIBUTE } BY { partition_expr | ( partition_expr [, ...] ) } }

table_ordering
  { { ORDER | SORT } BY { order_by_expr | ( order_by_expr [, ...] } }
```

### Parameters

- **function_name**

  The name of the built-in or user defined function. When resolving an unqualified function_name Spark will first consider a built-in or temporary function, and then a function in the current schema.

- **argExpr**

  Any expression which can be implicitly cast to the parameter it is associated with.

  The function may impose further restriction on the argument such as mandating literals, constant expressions, or specific values.

- **namedParameter**

  The unqualified name of a parameter to which the argExpr will be assigned.

  Named parameter notation is supported for Python UDF, and specific built-in functions.

- **table_argument**

  Specifies an argument for a parameter that is a table.

  - **TABLE ( table_name )**

    Identifies a table to pass to the function by name.

  - **TABLE ( query )**

    Passes the result of query to the function.

  - **table-partitioning**

    Optionally specifies that the table argument is partitioned. If not specified the partitioning is determined by Spark.

    - **WITH SINGLE PARTITION**

      The table argument is not partitioned.

    - **partition_expr**

      One or more expressions defining how to partition the table argument. Each expression can be composed of columns presents in the table argument, literals, parameters, variables, and deterministic functions.

    - **table_ordering**

      Optionally specifies an order in which the result rows of each partition of the table argument are passed to the function.

      By default, the order is undefined.

      - **order_by_expr**

        One or more expressions. Each expression can be composed of columns presents in the table argument, literals, parameters, variables, and deterministic functions.
