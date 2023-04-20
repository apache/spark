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

Convert a STRING literal into an identifier or name. The IDENTIFIER clause can be used in many places instead of, or in combination with identifiers where identifiers or names are used.
The purpose of this clause is to allow for templating of identifiers in SQL statements without opening up the risk of SQL injection attacks.

### Syntax

```sql
IDENTIFIER ( str )
```

### Parameters

* str: A STRING literal. Typically, the literal represented by a placeholder construct, such as a hive variable reference.

### Notes

While constant string expressions are not supported you can combine multiple string literals into one by chaining them: ‘hello’ ‘world’ is equivalent to ‘helloworld’.
This allows parameterization of partial strings such as ‘prefix_’ ${ hivevar:x } ‘_postfix’.

Different tools support different syntax for text substitution. For example a Notebook may use { { parameter } }.

### Examples

```sql
-- Using a Hive variable in Databricks Runtime
SET hivevar:tab = ‘T’;
CREATE TABLE IDENTIFIER(${hivevar:tab}) (c1 INT);
INSERT INTO IDENTIFIER(${hivevar:tab}) VALUES(1);
SELECT * FROM IDENTIFIER(${hivevar:tab});
 1
DROP TABLE IDENTIFIER(${hivevar:tab});

-- Using string coalescing:
SET hivevar:index = ‘1’;
CREATE TABLE IDENTIFIER(‘tab_’ ${hivevar:tab}) (c1 INT);
SELECT * FROM IDENTIFIER(‘tab_’ ${hivevar:tab});
DROP TABLE IDENTIFIER(‘tab_’ ${hivevar:tab});

-- Using a qualified name and backticks
SET hivevar:col = ‘`T`.c1’;
SELECT IDENTIFIER(${hivevar:col}) FROM VALUES(1) AS T(c1);
  1

-- In combination with a field reference
SET hivevar:col = ‘`T`.c1’;
SELECT IDENTIFIER(${hivevar:col}).a FROM VALUES(named_struct(‘a’, 1)) AS T(c1);
  1
```
