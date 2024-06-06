---
layout: global
title: Aggregate Functions
displayTitle: Aggregate Functions
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

Aggregate functions operate on values across rows to perform mathematical calculations such as sum, average, counting, minimum/maximum values, standard deviation, and estimation, as well as some non-mathematical operations.

### Syntax

```sql
aggregate_function(input1 [, input2, ...]) FILTER (WHERE boolean_expression)
```

### Parameters

* **aggregate_function**

    Please refer to the [Built-in Aggregation Functions](sql-ref-functions-builtin.html#aggregate-functions) document for a complete list of Spark aggregate functions.

* **boolean_expression**

    Specifies any expression that evaluates to a result type boolean. Two or more expressions may be combined together using the logical operators ( AND, OR ).

### Examples

Please refer to the [Built-in Aggregation Functions](sql-ref-functions-builtin.html#aggregate-functions) document for all the examples of Spark aggregate functions.

### Ordered-Set Aggregate Functions

These aggregate Functions use different syntax than the other aggregate functions so that to specify an expression (typically a column name) by which to order the values.

#### Syntax

```sql
{ PERCENTILE_CONT | PERCENTILE_DISC }(percentile) WITHIN GROUP (ORDER BY { order_by_expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ] }) FILTER (WHERE boolean_expression)
```

#### Parameters

* **percentile**

    The percentile of the value that you want to find. The percentile must be a constant between 0.0 and 1.0.

* **order_by_expression**

    The expression (typically a column name) by which to order the values before aggregating them.

* **boolean_expression**

    Specifies any expression that evaluates to a result type boolean. Two or more expressions may be combined together using the logical operators ( AND, OR ).

#### Examples

```sql
CREATE OR REPLACE TEMPORARY VIEW basic_pays AS SELECT * FROM VALUES
('Diane Murphy','Accounting',8435),
('Mary Patterson','Accounting',9998),
('Jeff Firrelli','Accounting',8992),
('William Patterson','Accounting',8870),
('Gerard Bondur','Accounting',11472),
('Anthony Bow','Accounting',6627),
('Leslie Jennings','IT',8113),
('Leslie Thompson','IT',5186),
('Julie Firrelli','Sales',9181),
('Steve Patterson','Sales',9441),
('Foon Yue Tseng','Sales',6660),
('George Vanauf','Sales',10563),
('Loui Bondur','SCM',10449),
('Gerard Hernandez','SCM',6949),
('Pamela Castillo','SCM',11303),
('Larry Bott','SCM',11798),
('Barry Jones','SCM',10586)
AS basic_pays(employee_name, department, salary);

SELECT * FROM basic_pays;
+-----------------+----------+------+
|    employee_name|department|salary|
+-----------------+----------+------+
|      Anthony Bow|Accounting|	6627|
|      Barry Jones|       SCM| 10586|
|     Diane Murphy|Accounting|	8435|
|   Foon Yue Tseng|     Sales|	6660|
|    George Vanauf|     Sales| 10563|
|    Gerard Bondur|Accounting| 11472|
| Gerard Hernandez|       SCM|	6949|
|    Jeff Firrelli|Accounting|	8992|
|   Julie Firrelli|     Sales|	9181|
|       Larry Bott|       SCM| 11798|
|  Leslie Jennings|        IT|	8113|
|  Leslie Thompson|        IT|	5186|
|      Loui Bondur|       SCM| 10449|
|   Mary Patterson|Accounting|	9998|
|  Pamela Castillo|       SCM| 11303|
|  Steve Patterson|     Sales|	9441|
|William Patterson|Accounting|	8870|
+-----------------+----------+------+

SELECT
    department,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary) AS pc1,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary) FILTER (WHERE employee_name LIKE '%Bo%') AS pc2,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary DESC) AS pc3,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary DESC) FILTER (WHERE employee_name LIKE '%Bo%') AS pc4,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary) AS pd1,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary) FILTER (WHERE employee_name LIKE '%Bo%') AS pd2,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary DESC) AS pd3,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary DESC) FILTER (WHERE employee_name LIKE '%Bo%') AS pd4
FROM basic_pays
GROUP BY department
ORDER BY department;
+----------+-------+--------+-------+--------+-----+-----+-----+-----+
|department|    pc1|     pc2|    pc3|     pc4|  pd1|  pd2|  pd3|  pd4|
+----------+-------+--------+-------+--------+-----+-----+-----+-----+
|Accounting|8543.75| 7838.25| 9746.5|10260.75| 8435| 6627| 9998|11472|
|        IT|5917.75|    NULL|7381.25|    NULL| 5186| NULL| 8113| NULL|
|     Sales|8550.75|    NULL| 9721.5|    NULL| 6660| NULL|10563| NULL|
|       SCM|10449.0|10786.25|11303.0|11460.75|10449|10449|11303|11798|
+----------+-------+--------+-------+--------+-----+-----+-----+-----+
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
