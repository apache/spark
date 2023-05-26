---
layout: global
title: Identifier clause
displayTitle: IDENTIFIER clause
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

Convert a constant `STRING` expression into a SQL object name.
The purpose of this clause is to allow for templating of identifiers in SQL statements without opening up the risk of SQL injection attacks.
Typically, this clause is used with a parameter marker as argument.

### Syntax

```sql
IDENTIFIER ( strExpr )
```

### Parameters

- strExpr: A constant STRING expression. Typically, the expression includes a parameter marker.

### Returns

A (qualified) identifier which can be used as a:

- qualified table name
- namespace name
- function name
- qualified column or attribute reference

### Examples

```scala
-- Using IDENTIFIER() on a namepace
spark.sql("CREATE SCHEMA IDENTIFIER(:namespace)", args = Map("namespace" -> "mychema")

-- Using IDENTIFIER() on table DDL
spark.sql("CREATE TABLE IDENTIFIER('myschema.' || :tabname) (c1 INT)", args = Map("tabname" -> "mytable")

-- Using IDENTIFIER() on table DML
spark.sql("INSERT INTO IDENTIFIER(:tabname) VALUES (1)", args = Map("tabname" -> "myschema.mytable")

-- Using IDENTIFIER() on a table reference 
spark.sql("SELECT * FROM IDENTIFIER(:tabname)", args = Map("tabname" -> "myschema.mytable")

-- Using IDENTIFIER() on a column reference 
spark.sql("SELECT IDENTIFIER(:colname) FROM mychema.mytable WHERE IDENTIFIER(:colname) IS NOT NULL", args = Map("colname" -> "`c1`")

-- Using IDENTIFIER() on a function reference 
spark.sql("SELECT IDENTIFIER(:aggFunc)(c1) FROM myschema.mytable", args = Map("aggFunc" -> "sum")

-- Using IDENTIFIER() on SHOW
spark.sql("SHOW TABLES IN IDENTIFIER(:namespace)", args = Map("namespace" -> "mychema")
```
