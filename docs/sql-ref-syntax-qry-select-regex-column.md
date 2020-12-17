---
layout: global
title: Regex Column Names
displayTitle: Regex Column Names
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

When `spark.sql.parser.quotedRegexColumnNames` is true, quoted Identifiers (using backticks) in SELECT statement are interpreted as regular expressions and SELECT statement can take regex-based column specification.

### Syntax

```sql
SELECT `regex` FROM ...
```

### Parameters
    
* **regex**

    Quoted regex pattern to represents all of the input attributes to a given relational operator.

### Examples

```sql
CREATE TABLE person (id INT, name STRING, age INT, class INT, address STRING);
INSERT INTO person VALUES
    (100, 'John', 30, 1, 'Street 1'),
    (200, 'Mary', NULL, 1, 'Street 2'),
    (300, 'Mike', 80, 3, 'Street 3'),
    (400, 'Dan', 50, 4, 'Street 4');
SET spark.sql.parser.quotedRegexColumnNames=true;

SELECT `(class|address)?+.+` FROM person;

+------+-------+
|  id  |  name |
+------+-------+
| 200  | John  |
| 100  | Mary  |
| 300  | Mike  |
| 400  | Dan   |
+------+-------+

SELECT `(id|name)` FROM person;
+------+-------+
|  id  |  name |
+------+-------+
| 200  | John  |
| 100  | Mary  |
| 300  | Mike  |
| 400  | Dan   |
+------+-------+
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
* [CASE Clause](sql-ref-syntax-qry-select-case.html)
* [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
