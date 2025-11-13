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

Converts a constant `STRING` expression into a SQL object name.
The purpose of this clause is to allow for templating of identifiers in SQL statements without opening up the risk of SQL injection attacks.

The clause comes in two forms:

- When passed a string-literal, which may include a coalesced string of literals and parameter markers, it can be used anywhere an identifier or qualified identifier can be used.
  The usage of this form is encouraged.
- When passed a more complex constant string expression, which may also include variables, it can be used in limited cases to reference dynamic SQL object references such as table names, column names, and function names.
  The usage of this form is discouraged unless necessary.
  For example you cannot use this form to parameterize a table or column alias, or a column name in a `CREATE TABLE` statement.

### Syntax

```sql
IDENTIFIER ( strLiteral )

IDENTIFIER ( strExpr )
```

### Parameters

- **strLiteral**

  A constant `STRING` literal which may include string coalescing of strings and string parameter markers.

  The string must be a valid (qualified) identifier.

  For example:

  ```
  IDENTIFIER('default.' :tablename)
  IDENTIFIER(:catalog '.' :namespace '.' :tablename '.' :columnname)
  IDENTIFIER(:rootdirectory '/data/' :tablename)
  ```

- **strExpr**: A constant `STRING` expression. Typically, the expression includes a parameter marker or session variable.

  The string must be a valid (qualified) identifier.

  For example:
  
  ```
  IDENTIFIER(session.schema || '.' || session.tablename)
  IDENTIFIER('`' || session.schema || '`.`' session.tablename || '`')
  IDENTIFIER(CONCAT(:catalog, '.', :namespace, '.', :tablename, '.', :columnname))
  ```

### Returns

A (qualified) identifier.

### Examples

#### Scala examples

These examples use named parameter markers to templatize queries.

```scala
// Creation of a table using parameter marker.
spark.sql("CREATE TABLE IDENTIFIER(:mytab)(c1 INT)", args = Map("mytab" -> "tab1")).show()

spark.sql("DESCRIBE IDENTIFIER(:mytab)", args = Map("mytab" -> "tab1")).show()
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|      c1|      int|   NULL|
+--------+---------+-------+

// Altering a table with a fixed schema and a parameterized table name. 
spark.sql("ALTER TABLE IDENTIFIER('default.' || :mytab) ADD COLUMN c2 INT", args = Map("mytab" -> "tab1")).show()

spark.sql("DESCRIBE IDENTIFIER(:mytab)", args = Map("mytab" -> "default.tab1")).show()
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|      c1|      int|   NULL|
|      c2|      int|   NULL|
+--------+---------+-------+

// A parameterized reference to a table in a query. This table name is qualified and uses back-ticks.
spark.sql("SELECT * FROM IDENTIFIER(:mytab)", args = Map("mytab" -> "`default`.`tab1`")).show()
+---+---+
| c1| c2|
+---+---+
+---+---+


// You cannot qualify the IDENTIFIER clause or use it as a qualifier itself.
spark.sql("SELECT * FROM myschema.IDENTIFIER(:mytab)", args = Map("mytab" -> "`tab1`")).show()
[INVALID_SQL_SYNTAX.INVALID_TABLE_VALUED_FUNC_NAME] `myschema`.`IDENTIFIER`.

spark.sql("SELECT * FROM IDENTIFIER(:myschema).mytab", args = Map("mychema" -> "`default`")).show()
[PARSE_SYNTAX_ERROR]

// Dropping a table with separate schema and table parameters.
spark.sql("DROP TABLE IDENTIFIER(:myschema '.' :mytab)", args = Map("myschema" -> "default", "mytab" -> "tab1")).show()

// A parameterized column reference
spark.sql("SELECT IDENTIFIER(:col) FROM VALUES(1) AS T(c1)", args = Map("col" -> "t.c1")).show()
+---+
| c1|
+---+
|  1|
+---+

// Passing in a function name as a parameter
spark.sql("SELECT IDENTIFIER(:func)(-1)", args = Map("func" -> "abs")).show();
+-------+
|abs(-1)|
+-------+
|      1|
+-------+
```

#### SQL examples

These examples use SQL variables to templatize queries.

```sql
DECLARE mytab = 'tab1';

-- Creation of a table using variable.
CREATE TABLE IDENTIFIER(mytab)(c1 INT);

EXECUTE IMMEDIATE 'DESCRIBE IDENTIFIER(:mytab)' USING mytab;
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|      c1|      int|   NULL|
+--------+---------+-------+

-- Altering a table with a fixed schema and a parameterized table name. 
EXECUTE IMMEDIATE 'ALTER TABLE IDENTIFIER('default.' || :mytab) ADD COLUMN :col INT' USING mytab, 'c2' AS col;

SET VAR mytab = '`default`.`tab1`';
EXECUTE IMMEDIATE 'DESCRIBE IDENTIFIER(:mytab)' USING mytab;
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|      c1|      int|   NULL|
|      c2|      int|   NULL|
+--------+---------+-------+

-- A parameterized reference to a table in a query. This table name is qualified and uses back-ticks.
EXECUTE IMMEDIATE 'SELECT * FROM IDENTIFIER(mytab)' USING mytab;
+---+---+
| c1| c2|
+---+---+

-- Dropping a table with separate schema and table parameters.
DECLARE myschema = 'default';
SET VAR mytab = 'tab1';
EXECUTE IMMEDIATE 'DROP TABLE IDENTIFIER(:myschema '.' :mytab)' USING myschema, mytab;

-- A parameterized column reference
DECLARE col = 'c1';
EXECUTE IMEMDIATE 'SELECT IDENTIFIER(:col) FROM VALUES(1) AS T(IDENTIFIER(:col))' USING col;
+---+
| c1|
+---+
|  1|
+---+

-- Passing in a function name as a parameter
EXECUTE IMMEDIATE 'SELECT IDENTIFIER(:func)(-1)' USING 'abs' AS func;
+-------+
|abs(-1)|
+-------+
|      1|
+-------+
```
