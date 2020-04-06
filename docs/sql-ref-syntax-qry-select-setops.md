---
layout: global
title: Set Operators
displayTitle: Set Operators
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

Set operators are used to combine the results of two queries into a single result. Spark SQL supports three types of set operators:
- `EXCEPT` and `EXCEPT ALL`
- `INTERSECT` and `INTERSECT ALL`
- `UNION` and `UNION ALL`

Note that the queries' result sets must have the same number of columns and compatible data types for the respective columns.

### EXCEPT and EXCEPT ALL
`EXCEPT` and `EXCEPT ALL` return the rows that are found in one query but not the other query. `EXCEPT` takes only distinct rows while `EXCEPT ALL` does not remove duplicates.

#### Syntax
{% highlight sql %}
[ ( ] query [ ) ] EXCEPT [ ALL ] [ ( ] query[ ) ]
{% endhighlight %}

### INTERSECT and INTERSECT ALL
`INTERSECT` and `INTERSECT ALL` return the rows that are found in both queries. `INTERSECT` takes only distinct rows while `INTERSECT ALL` does not remove duplicates.

#### Syntax
{% highlight sql %}
[ ( ] query [ ) ] INTERSECT [ ALL ] [ ( ] query [ ) ]
{% endhighlight %}

### UNION and UNION ALL
`UNION` and `UNION ALL` return the rows that are found in either query. `UNION` takes only distinct rows while `UNION ALL` does not remove duplicates.

#### Syntax
{% highlight sql %}
[ ( ] query [ ) ] UNION [ ALL ] [ ( ] query [ ) ]
{% endhighlight %}

### Examples
{% highlight sql %}
-- Use number1 and number2 tables to demonstrate set operators.
SELECT * FROM number1;
+---+
|  C|
+---+
|  3|
|  1|
|  2|
|  2|
|  3|
|  4|
+---+

SELECT * FROM number2;
+---+
|  C|
+---+
|  5|
|  1|
|  2|
|  2|
+---+

SELECT C from number1 EXCEPT SELECT C FROM number2;
+---+
|  C|
+---+
|  3|
|  4|
+---+

SELECT C from number1 EXCEPT ALL (SELECT C FROM number2);
+---+
|  C|
+---+
|  3|
|  3|
|  4|
+---+

(SELECT C from number1) INTERSECT (SELECT C FROM number2);
+---+
|  C|
+---+
|  1|
|  2|
+---+

(SELECT C from number1) INTERSECT ALL (SELECT C FROM number2);
+---+
|  C|
+---+
|  1|
|  2|
|  2|
+---+

(SELECT C from number1) UNION (SELECT C FROM number2);
+---+
|  C|
+---+
|  1|
|  3|
|  5|
|  4|
|  2|
+---+

SELECT C from number1 UNION ALL (SELECT C FROM number2);
+---+
|  C|
+---+
|  3|
|  1|
|  2|
|  2|
|  3|
|  4|
|  5|
|  1|
|  2|
|  2|
+---+

{% endhighlight %}

### Related Statement
- [SELECT Statement](sql-ref-syntax-qry-select.html)

