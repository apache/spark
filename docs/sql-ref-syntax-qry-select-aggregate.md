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

### General Aggregation

Please refer to the [Built-in Aggregation Functions](sql-ref-functions-builtin.html#aggregate-functions) document for a complete list of Spark aggregate functions.

### Ordered-Set Aggregate Functions

These aggregate Functions use different syntax than the other aggregate functions so that to specify an expression (typically a column name) by which to order the values.

#### Syntax

```sql
{ PERCENTILE_CONT | PERCENTILE_DISC }(percentile) WITHIN GROUP (ORDER BY order_by_expr) OVER
( [  PARTITION BY partition_col_name = partition_col_val ( [ , ... ] ) ])
```

#### Parameters

* **percentile**

    The percentile of the value that you want to find. The percentile must be a constant between 0.0 and 1.0.

* **order_by_expr**

    The expression (typically a column name) by which to order the values.

* **partition_col_name**

    This is the optional expression used to group rows into partitions.

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
|      Barry Jones|	      SCM| 10586|
|     Diane Murphy|Accounting|	8435|
|   Foon Yue Tseng|	    Sales|	6660|
|    George Vanauf|	    Sales| 10563|
|    Gerard Bondur|Accounting| 11472|
| Gerard Hernandez|	      SCM|	6949|
|    Jeff Firrelli|Accounting|	8992|
|   Julie Firrelli|	    Sales|	9181|
|       Larry Bott|	      SCM| 11798|
|  Leslie Jennings|        IT|	8113|
|  Leslie Thompson|	       IT|	5186|
|      Loui Bondur|	      SCM| 10449|
|   Mary Patterson|Accounting|	9998|
|  Pamela Castillo|	      SCM| 11303|
|  Steve Patterson|	    Sales|	9441|
|William Patterson|Accounting|	8870|
+-----------------+----------+------+

SELECT
    employee_name,
    department,
    salary,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary) OVER w AS p1,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary) OVER w AS p2,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER w AS p3,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY salary DESC) OVER w AS p4
FROM basic_pays
WINDOW w AS (PARTITION BY department)
ORDER BY salary;
+-----------------+----------+------+-------+-------+-------+-------+
|    employee_name|department|salary|     p1|     p2|     p3|     p4|
+-----------------+----------+------+-------+-------+-------+-------+
|  Leslie Thompson|        IT|  5186|5917.75| 5186.0|7381.25| 8113.0|
|      Anthony Bow|Accounting|  6627|8543.75| 8435.0| 9746.5| 9998.0|
|   Foon Yue Tseng|	    Sales|	6660|8550.75| 6660.0| 9721.5|10563.0|
| Gerard Hernandez|	      SCM|	6949|10449.0|10449.0|11303.0|11303.0|
|  Leslie Jennings|	       IT|	8113|5917.75| 5186.0|7381.25| 8113.0|
|     Diane Murphy|Accounting|	8435|8543.75| 8435.0| 9746.5| 9998.0|
|William Patterson|Accounting|	8870|8543.75| 8435.0| 9746.5| 9998.0|
|    Jeff Firrelli|Accounting|	8992|8543.75| 8435.0| 9746.5| 9998.0|
|   Julie Firrelli|	    Sales|	9181|8550.75| 6660.0| 9721.5|10563.0|
|  Steve Patterson|	    Sales|	9441|8550.75| 6660.0| 9721.5|10563.0|
|   Mary Patterson|Accounting|	9998|8543.75| 8435.0| 9746.5| 9998.0|
|      Loui Bondur|	      SCM| 10449|10449.0|10449.0|11303.0|11303.0|
|    George Vanauf|	    Sales| 10563|8550.75| 6660.0| 9721.5|10563.0|
|      Barry Jones|	      SCM| 10586|10449.0|10449.0|11303.0|11303.0|
|  Pamela Castillo|	      SCM| 11303|10449.0|10449.0|11303.0|11303.0|
|    Gerard Bondur|Accounting| 11472|8543.75| 8435.0| 9746.5| 9998.0|
|       Larry Bott|    	  SCM| 11798|10449.0|10449.0|11303.0|11303.0|
+-----------------+----------+------+-------+-------+-------+-------+
```

### Related Statements

* [SELECT](sql-ref-syntax-qry-select.html)
