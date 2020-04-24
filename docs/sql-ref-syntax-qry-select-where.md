---
layout: global
title: SELECT
displayTitle: WHERE clause
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

The <code>WHERE</code> clause is used to limit the results of the <code>FROM</code>
clause of a query or a subquery based on the specified condition.

### Syntax

{% highlight sql %}
WHERE boolean_expression
{% endhighlight %}

### Parameters

<dl>
  <dt><code><em>boolean_expression</em></code></dt>
  <dd>
    Specifies any expression that evaluates to a result type <code>boolean</code>. Two or
    more expressions may be combined together using the logical
    operators ( <code>AND</code>, <code>OR</code> ). <br><br>
    <b>Syntax:</b><br>
      <code>
        NOT boolean_expression | EXISTS ( query ) | column_name LIKE regex_pattern | value_expression |<br>
        boolean_expression AND boolean_expression | boolean_expression OR boolean_expression
      </code>
    <dl>
      <dt><code><em>query</em></code></dt>
      <dd>
        Specifies a <a href="sql-ref-syntax-qry-select.html">select statement</a>.
      </dd>
      <dt><code><em>regex_pattern</em></code></dt>
      <dd>
         Specifies the regular expression pattern that is used to limit the results of the statement.
         <ul>
            <li> Except for <code>*</code> and <code>|</code> character, the pattern works like a regex.</li>
            <li>  <code>* </code> alone matches 0 or more characters and  <code>|</code> is used to separate multiple different regexes,
             any of which can match. </li>
            <li> The leading and trailing blanks are trimmed in the input pattern before processing.</li>
         </ul>
      </dd>
      <dt><code><em>value_expression</em></code></dt>
      <dd>
        Specifies a combination of one or more values, operators, and SQL functions that evaluates to a value.
      </dd>
    </dl>
  </dd>
</dl>

### Examples

{% highlight sql %}
CREATE TABLE person (id INT, name STRING, age INT);
INSERT INTO person VALUES
    (100, 'John', 30),
    (200, 'Mary', NULL),
    (300, 'Mike', 80),
    (400, 'Dan',  50);

-- Comparison operator in `WHERE` clause.
SELECT * FROM person WHERE id > 200 ORDER BY id;
  +---+----+---+
  | id|name|age|
  +---+----+---+
  |300|Mike| 80|
  |400| Dan| 50|
  +---+----+---+

-- Comparison and logical operators in `WHERE` clause.
SELECT * FROM person WHERE id = 200 OR id = 300 ORDER BY id;
  +---+----+----+
  | id|name| age|
  +---+----+----+
  |200|Mary|null|
  |300|Mike|  80|
  +---+----+----+

-- `LIKE` in `WHERE` clause.
SELECT * FROM person WHERE name LIKE 'M%';
  +---+----+---+
  | id|name|age|
  +---+----+---+
  |200|Mary|null|
  |300|Mike| 80|
  +---+----+---+

-- IS NULL expression in `WHERE` clause.
SELECT * FROM person WHERE id > 300 OR age IS NULL ORDER BY id;
  +---+----+----+
  | id|name| age|
  +---+----+----+
  |200|Mary|null|
  |400| Dan|  50|
  +---+----+----+

-- Function expression in `WHERE` clause.
SELECT * FROM person WHERE length(name) > 3 ORDER BY id;
  +---+----+----+
  | id|name| age|
  +---+----+----+
  |100|John|  30|
  |200|Mary|null|
  |300|Mike|  80|
  +---+----+----+

-- `BETWEEN` expression in `WHERE` clause.
SELECT * FROM person WHERE id BETWEEN 200 AND 300 ORDER BY id;
  +---+----+----+
  | id|name| age|
  +---+----+----+
  |200|Mary|null|
  |300|Mike|  80|
  +---+----+----+

-- Scalar Subquery in `WHERE` clause.
SELECT * FROM person WHERE age > (SELECT avg(age) FROM person);
  +---+----+---+
  | id|name|age|
  +---+----+---+
  |300|Mike| 80|
  +---+----+---+

-- Correlated Subquery in `WHERE` clause.
SELECT * FROM person AS parent
    WHERE EXISTS (
        SELECT 1 FROM person AS child
        WHERE parent.id = child.id AND child.age IS NULL
    );
  +---+----+----+
  |id |name|age |
  +---+----+----+
  |200|Mary|null|
  +---+----+----+
{% endhighlight %}

### Related Statements

 * [SELECT Main](sql-ref-syntax-qry-select.html)
 * [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
 * [HAVING Clause](sql-ref-syntax-qry-select-having.html)
 * [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
 * [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
 * [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
 * [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
 * [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
