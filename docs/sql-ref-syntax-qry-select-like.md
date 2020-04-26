---
layout: global
title: LIKE Predicate
displayTitle: LIKE Predicate
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

A LIKE predicate is used to search for a specific pattern.

### Syntax

{% highlight sql %}
[ NOT ] { LIKE search_pattern [ ESCAPE esc_char ] | RLIKE regex_pattern }
{% endhighlight %}

### Parameters

<dl>
  <dt><code><em>search_pattern</em></code></dt>
  <dd>
    Specifies a string pattern to be searched by the <code>LIKE</code> clause. It can contain special pattern-matching characters:
    <ul>
      <li><code>%</code></li> matches zero or more characters.
      <li><code>_</code></li> matches exactly one character.
    </ul>
  </dd>
</dl>
<dl>
  <dt><code><em>esc_char</em></code></dt>
  <dd>
    Specifies the escape character. The default escape character is <code>\</code>.
  </dd>
</dl>
<dl>
  <dt><code><em>regex_pattern</em></code></dt>
  <dd>
    Specifies a regular expression search pattern to be searched by the <code>RLIKE</code> clause.
  </dd>
</dl>

### Examples

{% highlight sql %}
CREATE TABLE person (id INT, name STRING, age INT);
INSERT INTO person VALUES
    (100, 'John', 30),
    (200, 'Mary', NULL),
    (300, 'Mike', 80),
    (400, 'Dan',  50),
    (500, 'Evan_w', 16);

SELECT * FROM person WHERE name LIKE 'M%';
+---+----+----+
| id|name| age|
+---+----+----+
|300|Mike|  80|
|200|Mary|null|
+---+----+----+

SELECT * FROM person WHERE name LIKE 'M_ry';
+---+----+----+
| id|name| age|
+---+----+----+
|200|Mary|null|
+---+----+----+

SELECT * FROM person WHERE name NOT LIKE 'M_ry';
+---+------+---+
| id|  name|age|
+---+------+---+
|500|Evan_W| 16|
|300|  Mike| 80|
|100|  John| 30|
|400|   Dan| 50|
+---+------+---+

SELECT * FROM person WHERE name RLIKE '[MD]';
+---+----+----+
| id|name| age|
+---+----+----+
|300|Mike|  80|
|400| Dan|  50|
|200|Mary|null|
+---+----+----+

SELECT * FROM person WHERE name LIKE '%\_%';
+---+------+---+
| id|  name|age|
+---+------+---+
|500|Evan_W| 16|
+---+------+---+

SELECT * FROM person WHERE name LIKE '%$_%' ESCAPE '$';
+---+------+---+
| id|  name|age|
+---+------+---+
|500|Evan_W| 16|
+---+------+---+
{% endhighlight %}

### Related Statements

 * [SELECT](sql-ref-syntax-qry-select.html)
 * [WHERE Clause](sql-ref-syntax-qry-select-where.html)
