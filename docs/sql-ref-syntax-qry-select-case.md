---
layout: global
title: CASE Clause
displayTitle: CASE Clause
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

`CASE` clause uses rule to return specific result based on the specified condition.

### Syntax

```sql
CASE { WHEN boolean_expression THEN then_expression }{ WHEN boolean_expression THEN then_expression } [ , ... ] [ ELSE else_expression ] END

CASE expression { WHEN boolean_expression THEN then_expression }{ WHEN boolean_expression THEN then_expression } [ , ... ] [ ELSE else_expression ] END
```

### Parameters
    
* **WHEN**

    Specific a boolean condition ,under which to return the `THEN` result, `WHEN` must exist in `CASE` clause.
    
* **THEN**

    Specific a result base the `WHEN` condition, `THEN` must exist in `CASE` clause.
    
* **ELSE**

    Specific a default result for the `CASE` rules, it is optional, if user don't use else then the `CASE` will not have default result.
    
* **END**

    Key words to finish a case clause, `END` must exist in `CASE` clause.
    
* **boolean_expression**

    Specific specified condition, it should be boolean type.
    
* **then_expression**

    Specific the then expression based on the `boolean_expression` condition, `then_expression` and `else_expression` should all be same type or coercible to a common type.
    
* **else_expression**

    Specific the default expression, `then_expression` and `else_expression` should all be same type or coercible to a common type.
    
### Examples

```sql
CREATE TABLE person (id INT, name STRING, age INT);
INSERT INTO person VALUES
    (100, 'John', 30),
    (200, 'Mary', NULL),
    (300, 'Mike', 80),
    (400, 'Dan',  50),
    (500, 'Evan_w', 16);

select id, case when id > 200 then 'bigger' else 'small' end from person;
+------+--------------------------------------------------+--+
|  id  | CASE WHEN (id > 200) THEN bigger ELSE small END  |
+------+--------------------------------------------------+--+
| 100  | small                                            |
| 200  | small                                            |
| 300  | bigger                                           |
| 400  | bigger                                           |
+------+--------------------------------------------------+--+

select id, case id when 100 then 'bigger' when  id > 300 then '300' else 'small' end from person;
+------+-----------------------------------------------------------------------------------------------+--+
|  id  | CASE WHEN (id = 100) THEN bigger WHEN (id = CAST((id > 300) AS INT)) THEN 300 ELSE small END  |
+------+-----------------------------------------------------------------------------------------------+--+
| 100  | bigger                                                                                        |
| 200  | small                                                                                         |
| 300  | small                                                                                         |
| 400  | small                                                                                         |
+------+-----------------------------------------------------------------------------------------------+--+

select * from person where case 1 = 1 when 100 then 'big' when 200 then 'bigger' when  300 then 'biggest' else 'small' end = 'small';
+------+-------+-------+--+
|  id  | name  |  age  |
+------+-------+-------+--+
| 100  | John  | 30    |
| 200  | Mary  | NULL  |
| 300  | Mike  | 80    |
| 400  | Dan   | 50    |
+------+-------+-------+--+
```

### Related Statements

* [SELECT Main](sql-ref-syntax-qry-select.html)
* [WHERE Clause](sql-ref-syntax-qry-select-where.html)
* [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
* [HAVING Clause](sql-ref-syntax-qry-select-having.html)
* [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
* [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
* [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
* [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
