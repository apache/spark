---
layout: global
title: Identifiers
displayTitle: Identifiers
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

An identifier is a string used to identify a database object such as a table, view, schema, column, etc. Spark SQL has regular identifiers and delimited identifiers, which are enclosed within backticks. Both regular identifiers and delimited identifiers are case-insensitive.

### Syntax

#### Regular Identifier

```sql
{ letter | digit | '_' } [ , ... ]
```
**Note:** If `spark.sql.ansi.enabled` is set to true, ANSI SQL reserved keywords cannot be used as identifiers. For more details, please refer to [ANSI Compliance](sql-ref-ansi-compliance.html).

#### Delimited Identifier

```sql
`c [ ... ]`
```

### Parameters

* **letter**

    Any letter from A-Z or a-z.

* **digit**

    Any numeral from 0 to 9.

* **c**

    Any character from the character set. Use <code>`</code> to escape special characters (e.g., <code>`</code>).

### Examples

```sql
-- This CREATE TABLE fails with ParseException because of the illegal identifier name a.b
CREATE TABLE test (a.b int);
Error in query:
[PARSE_SYNTAX_ERROR] Syntax error at or near '.': extra input '.'(line 1, pos 20)

== SQL ==
CREATE TABLE test (a.b int)
--------------------^^^

-- This CREATE TABLE works
CREATE TABLE test (`a.b` int);

-- This CREATE TABLE fails with ParseException because special character ` is not escaped
CREATE TABLE test1 (`a`b` int);
Error in query:
[PARSE_SYNTAX_ERROR] Syntax error at or near '`'(line 1, pos 24)

== SQL ==
CREATE TABLE test1 (`a`b` int)
------------------------^^^

-- This CREATE TABLE works
CREATE TABLE test (`a``b` int);
```
