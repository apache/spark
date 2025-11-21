---
layout: global
title: SQL Scripting
displayTitle: SQL Scripting
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

You can employ powerful procedural logic using SQL/PSM standard-based scripting syntax.
Any SQL script consists of and starts with a [compound statement](control-flow/compound-stmt.html) block (`BEGIN ... END`).
A compound statement starts with a section to declare local variables, user-defined conditions, and condition handlers, which are used to catch exceptions.
This is followed by the compound statement body, which consists of:

- Flow control statements include loops over predicate expressions, [FOR](control-flow/for-stmt.html) loops over query results, conditional logic such as [IF](control-flow/if-stmt.html) and [CASE](control-flow/case-stmt.html), and means to break out loops such as [LEAVE](control-flow/leave-stmt.html) and [ITERATE](control-flow/iterate-stmt.html).
- DDL statements such as `ALTER`, `CREATE`, `DROP`.
- DML statements [INSERT](sql-ref-syntax-dml-insert-into.html).
- [Queries](sql-ref-syntax-qry-select.html) that return result sets to the invoker of the script.
- [SET](sql-ref-syntax-aux-set-var.html) statements to set local variables as well as session variables.
- The [EXECUTE IMMEDIATE](sql-ref-syntax-aux-exec-imm.html) statement.
- Nested compound statements, which provide nested scopes for variables, conditions, and condition handlers.

## Passing data between the invoker and the compound statement

There are two ways to pass data to and from a SQL script:

- Use session variables to pass scalar values or small sets of arrays or maps from one SQL script to another.
- Use parameter markers to pass scalar values or small sets of arrays or map data from a notebook widget, Python, or another language to the SQL Script.

## Variable scoping

Variables declared within a compound statement can be referenced in any expression within a compound statement.
Spark resolves identifiers from the innermost scope outward, following the rules described in [Name Resolution](sql-ref-name-resolution.html).
You can use the optional compound statement labels to disambiguate duplicate variable names.

## Condition handling

SQL Scripting supports condition handlers, which are used to intercept and process exceptions to `EXIT` processing of the SQL script.

Condition handlers can be defined to handle three distinct classes of conditions:

- One or more named conditions that can be a specific Spark-defined error class such as `DIVIDE_BY_ZERO` or a user-declared condition.
  These handlers handle these specific conditions.

- One or more `SQLSTATE`s, that can be raised by Spark.
  These handlers can handle any condition associated with that `SQLSTATE`.

- A generic `SQLEXCEPTION` handler can catch all conditions falling into the `SQLEXCEPTION` (any `SQLSTATE` which is not `XX***` and not `02***`).

The following are used to decide which condition handler applies to an exception.
This condition handler is called the **most appropriate handler**:

- A condition handler cannot apply to any statement defined in its own body or the body of any condition handler declared in the same compound statement.

- The applicable condition handlers defined in the innermost compound statement within which the exception was raised are appropriate.

- If more than one appropriate handler is available, the most specific handler is the most appropriate.
  For example, a handler on a named condition is more specific than one on a named `SQLSTATE`.
  A generic `EXCEPTION` handler is the least specific.

The outcome of a condition handler is to execute the statement following the compound statement that declared the handler to execute next.

The following is a list of supported control flow statements:

* [CASE](control-flow/case-stmt.html)
* [compound statement](control-flow/compound-stmt.html)
* [FOR](control-flow/for-stmt.html)
* [IF](control-flow/if-stmt.html)
* [ITERATE](control-flow/iterate-stmt.html)
* [LEAVE](control-flow/leave-stmt.html)
* [LOOP](control-flow/loop-stmt.html)
* [REPEAT](control-flow/repeat-stmt.html)
* [WHILE](control-flow/while-stmt.html)
