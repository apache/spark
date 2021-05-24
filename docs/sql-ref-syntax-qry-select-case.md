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

`CASE` clause uses a rule to return a specific result based on the specified condition, similar to if/else statements in other programming languages.

### Syntax

```sql
CASE [ expression ] { WHEN boolean_expression THEN then_expression } [ ... ]
    [ ELSE else_expression ]
END
```

### Parameters
    
* **boolean_expression**

    Specifies any expression that evaluates to a result type `boolean`. Two or
    more expressions may be combined together using the logical
    operators ( `AND`, `OR` ).

* **then_expression**

    Specifies the then expression based on the `boolean_expression` condition; `then_expression` and `else_expression` should all be same type or coercible to a common type.
    
* **else_expression**

    Specifies the default expression; `then_expression` and `else_expression` should all be same type or coercible to a common type.
    
### Examples

```sql
CREATE TABLE person (id INT, name STRING, age INT);
INSERT INTO person VALUES
    (100, 'John', 30),
    (200, 'Mary', NULL),
    (300, 'Mike', 80),
    (400, 'Dan', 50);

SELECT id, CASE WHEN id > 200 THEN 'bigger' ELSE 'small' END FROM person;
+------+--------------------------------------------------+
|  id  | CASE WHEN (id > 200) THEN bigger ELSE small END  |
+------+--------------------------------------------------+
| 100  | small                                            |
| 200  | small                                            |
| 300  | bigger                                           |
| 400  | bigger                                           |
+------+--------------------------------------------------+

SELECT id, CASE id WHEN 100 then 'bigger' WHEN  id > 300 THEN '300' ELSE 'small' END FROM person;
+------+-----------------------------------------------------------------------------------------------+
|  id  | CASE WHEN (id = 100) THEN bigger WHEN (id = CAST((id > 300) AS INT)) THEN 300 ELSE small END  |
+------+-----------------------------------------------------------------------------------------------+
| 100  | bigger                                                                                        |
| 200  | small                                                                                         |
| 300  | small                                                                                         |
| 400  | small                                                                                         |
+------+-----------------------------------------------------------------------------------------------+

SELECT * FROM person
    WHERE 
        CASE 1 = 1 
            WHEN 100 THEN 'big' 
            WHEN 200 THEN 'bigger'
            WHEN 300 THEN 'biggest' 
            ELSE 'small'
        END = 'small';
+------+-------+-------+
|  id  | name  |  age  |
+------+-------+-------+
| 100  | John  | 30    |
| 200  | Mary  | NULL  |
| 300  | Mike  | 80    |
| 400  | Dan   | 50    |
+------+-------+-------+
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
* [PIVOT Clause](sql-ref-syntax-qry-select-pivot.html)
* [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
