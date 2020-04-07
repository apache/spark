---
layout: global
title: Built-in Aggregate Functions
displayTitle: Built-in Aggregate Functions
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

Spark SQL provides build-in aggregate functions defined in the dataset API and SQL interface. Aggregate functions
operate on a group of rows and return a single value.

<table class="table">
  <thead>
    <tr><th style="width:25%">Function</th><th>Argument Type(s)</th><th>Description</th></tr>
  </thead>
  <tbody>
    <tr>
      <td><b>{any | some | bool_or}</b>(<i>expression</i>)</td>
      <td>boolean</td>
      <td>Returns true if at least one value is true.</td>
    </tr>
    <tr>
      <td><b>approx_count_distinct</b>(<i>expression[, relativeSD]</i>)</td>
      <td>(long, double)</td>
      <td>`relativeSD` is the maximum estimation error allowed. Returns the estimated cardinality by HyperLogLog++.</td>
    </tr>   
    <tr>
      <td><b>{avg | mean}</b>(<i>expression</i>)</td>
      <td>numeric or string</td>
      <td>Returns the average of values in the input expression.</td> 
    </tr>
    <tr>
      <td><b>{bool_and | every}</b>(<i>expression</i>)</td>
      <td>boolean</td>
      <td>Returns true if all values are true.</td>
    </tr>
    <tr>
      <td><b>collect_list</b>(<i>expression</i>)</td>
      <td>any</td>
      <td>Collects and returns a list of non-unique elements. The function is non-deterministic because the order of collected results depends on the order of the rows which may be non-deterministic after a shuffle.</td>
    </tr>       
    <tr>
      <td><b>collect_set</b>(<i>expression</i>)</td>
      <td>any</td>
      <td>Collects and returns a set of unique elements. The function is non-deterministic because the order of collected results depends on the order of the rows which may be non-deterministic after a shuffle.</td>
    </tr>
    <tr>
      <td><b>corr</b>(<i>expression1, expression2</i>)</td>
      <td>(double, double)</td>
      <td>Returns Pearson coefficient of correlation between a set of number pairs.</td>
    </tr>
    <tr>
      <td><b>count</b>([<b>DISTINCT</b>] <i>*</i>)</td>
      <td>none</td>
      <td>If specified <code>DISTINCT</code>, returns the total number of retrieved rows are unique and not null; Otherwise, returns the total number of retrieved rows, including rows containing null.</td>
    </tr>
    <tr>
      <td><b>count</b>([<b>DISTINCT</b>] <i>expression1[, expression2</i>])</td>
      <td>(any, any)</td>
      <td>If specified <code>DISTINCT</code>, returns the number of rows for which the supplied expression(s) are unique and not null; Otherwise, returns the number of rows for which the supplied expression(s) are all not null.</td>
    </tr>
    <tr>
      <td><b>count_if</b>(<i>predicate</i>)</td>
      <td>expression that will be used for aggregation calculation</td>
      <td>Returns the count number from the predicate evaluate to `TRUE` values.</td>
    </tr> 
    <tr>
      <td><b>count_min_sketch</b>(<i>expression, eps, confidence, seed</i>)</td>
      <td>(integer or string or binary, double,  double, integer)</td>
      <td>`eps` and `confidence` are the double values between 0.0 and 1.0, `seed` is a positive integer. Returns a count-min sketch of a expression with the given `esp`, `confidence` and `seed`. The result is an array of bytes, which can be deserialized to a `CountMinSketch` before usage. Count-min sketch is a probabilistic data structure used for cardinality estimation using sub-linear space.</td>
    </tr>
    <tr>
      <td><b>covar_pop</b>(<i>expression1, expression2</i>)</td>
      <td>(double, double)</td>
      <td>Returns the population covariance of a set of number pairs.</td>
    </tr> 
    <tr>
      <td><b>covar_samp</b>(<i>expression1, expression2</i>)</td>
      <td>(double, double)</td>
      <td>Returns the sample covariance of a set of number pairs.</td>
    </tr>  
    <tr>
      <td><b>{first | first_value}</b>(<i>expression[, isIgnoreNull]</i>)</td>
      <td>(any, boolean)</td>
      <td>Returns the first value of expression for a group of rows. If <code>isIgnoreNull</code> is true, returns only non-null values, default is false. This function is non-deterministic.</td>
    </tr>      
    <tr>
      <td><b>kurtosis</b>(<i>expression</i>)</td>
      <td>double</td>
      <td>Returns the kurtosis value calculated from values of a group.</td>
    </tr>    
    <tr>
      <td><b>{last | last_value}</b>(<i>expression[, isIgnoreNull]</i>)</td>
      <td>(any, boolean)</td>
      <td>Returns the last value of expression for a group of rows. If <code>isIgnoreNull</code> is true, returns only non-null values, default is false. This function is non-deterministic.</td>
    </tr>      
    <tr>
      <td><b>max</b>(<i>expression</i>)</td>
      <td>any numeric, string, datetime or arrays of these types</td>
      <td>Returns the maximum value of the expression.</td>
    </tr>          
    <tr>
      <td><b>max_by</b>(<i>expression1, expression2</i>)</td>
      <td>any numeric, string, datetime or arrays of these types</td>
      <td>Returns the value of expression1 associated with the maximum value of expression2.</td>
    </tr>   
    <tr>
      <td><b>min</b>(<i>expression</i>)</td>
      <td>any numeric, string, datetime or arrays of these types</td>
      <td>Returns the minimum value of the expression.</td>
    </tr>          
    <tr>
      <td><b>min_by</b>(<i>expression1, expression2</i>)</td>
      <td>any numeric, string, datetime or arrays of these types</td>
      <td>Returns the value of expression1 associated with the minimum value of expression2.</td>
    </tr>      
    <tr>
      <td><b>percentile</b>(<i>expression, percentage [, frequency]</i>)</td>
      <td>numeric, double, integer</td>
      <td>`percentage` is a number between 0 and 1; `frequency` is a positive integer. Returns the exact percentile value of numeric expression at the given percentage.</td>
    </tr>         
    <tr>
      <td><b>percentile</b>(<i>expression, <b>array</b>(percentage1 [, percentage2]...) [, frequency]</i>)</td>
      <td>numeric, double, integer</td>
      <td>Percentage array is an array of number between 0 and 1; `frequency` is a positive integer. Returns the exact percentile value array of numeric expression at the given percentage(s).</td>
    </tr>        
    <tr>
      <td><b>{percentile_approx | percentile_approx}</b>(<i>expression, percentage [, frequency]</i>)</td>
      <td>numeric, double, integer</td>
      <td>`percentage` is a number between 0 and 1; `frequency` is a positive integer. Returns the approximate percentile value of numeric expression at the given percentage.</td>
    </tr>    
   <tr>
      <td><b>{percentile_approx | percentile_approx}</b>(<i>expression, percentage [, frequency]</i>)</td>
      <td>datetime, double, integer</td>
      <td>`percentage` is a number between 0 and 1; `frequency` is a positive integer. Returns the approximate percentile value of numeric expression at the given percentage.</td>
    </tr>                  
    <tr>
      <td><b>{percentile_approx | percentile_approx}</b>(<i>expression, <b>array</b>(percentage1 [, percentage2]...) [, frequency]</i>)</td>
      <td>numeric, double, integer</td>
      <td>`percentage` is a number between 0 and 1; `frequency` is a positive integer. Returns the approximate percentile value of numeric expression at the given percentage.</td>
    </tr>             
    <tr>
      <td><b>{percentile_approx | percentile_approx}</b>(<i>expression, <b>array</b>(percentage1 [, percentage2]...) [, frequency]</i>)</td>
      <td>datetime , double, integer</td>
      <td>`percentage` is a number between 0 and 1; `frequency` is a positive integer. Returns the approximate percentile value of numeric expression at the given percentage.</td>
    </tr>             
    <tr>
      <td><b>skewness</b>(<i>expression</i>)</td>
      <td>double</td>
      <td>Returns the skewness value calculated from values of a group.</td>
    </tr>    
    <tr>
      <td><b>{stddev_samp | stddev | std}</b>(<i>expression</i>)</td>
      <td>double</td>
      <td>Returns the sample standard deviation calculated from values of a group.</td>
    </tr>  
    <tr>
      <td><b>stddev_pop</b>(<i>expression</i>)</td>
      <td>double</td>
      <td>Returns the population standard deviation calculated from values of a group.</td>
    </tr>
    <tr>
      <td><b>sum</b>(<i>expression</i>)</td>
      <td>numeric</td>
      <td>Returns the sum calculated from values of a group.</td>
    </tr>       
    <tr>
      <td><b>{variance | var_samp}</b>(<i>expression</i>)</td>
      <td>double</td>
      <td>Returns the sample variance calculated from values of a group.</td>
    </tr>    
    <tr>
      <td><b>var_pop</b>(<i>expression</i>)</td>
      <td>double</td>
      <td>Returns the population variance calculated from values of a group.</td>
    </tr>        
  </tbody>
</table>

### Examples
{% highlight sql %}
--base table 

SELECT * FROM buildin_agg;
+----+----+----+-----+----+
|  c1|  c2|  c3|   c4|  c5|
+----+----+----+-----+----+
|   2|   3|agg4| true|true|
|   1|   2|agg3|false|true|
|   1|   1|agg1|false|true|
|   4|   3|agg6|false|true|
|   3|   3|agg5| true|true|
|   1|   2|agg2|false|true|
|   5|null|agg8|false|true|
|null|   4|agg7|false|true|
+----+----+----+-----+----+

-- any, some and bool_or

SELECT ANY(c4) FROM buildin_agg;
+-------+
|any(c4)|
+-------+
|   true|
+-------+

SELECT SOME(c4) FROM buildin_agg;
+-------+
|any(c4)|
+-------+
|   true|
+-------+

SELECT BOOL_OR(c5) FROM buildin_agg;
+-----------+
|bool_or(c5)|
+-----------+
|       true|
+-----------+

-- approx_count_distinct

SELECT APPROX_COUNT_DISTINCT(c1) FROM buildin_agg;
+-------------------------+
|approx_count_distinct(c1)|
+-------------------------+
|                        5|
+-------------------------+

SELECT APPROX_COUNT_DISTINCT(c1,0.39) FROM buildin_agg;
+-------------------------+
|approx_count_distinct(c1)|
+-------------------------+
|                        6|
+-------------------------+

-- avg and mean

SELECT AVG(c1) FROM buildin_agg;
+------------------+
|           avg(c1)|
+------------------+
|2.4285714285714284|
+------------------+

SELECT MEAN(c1) FROM buildin_agg;
+------------------+
|          mean(c1)|
+------------------+
|2.4285714285714284|
+------------------+

-- bool_and and every
 
SELECT BOOL_AND(c4) FROM buildin_agg;
+------------+
|bool_and(c4)|
+------------+
|       false|
+------------+

SELECT EVERY(c5) FROM buildin_agg;
+------------+
|bool_and(c5)|
+------------+
|        true|
+------------+

--collect_list

SELECT COLLECT_LIST(c2) FROM buildin_agg;
+---------------------+
|collect_list(c2)     |
+---------------------+
|[3, 2, 1, 3, 3, 2, 4]|
+---------------------+

SELECT COLLECT_LIST(c4) FROM buildin_agg;
+------------------------------------------------------+
|collect_list(c4)                                      |
+------------------------------------------------------+
|[true, false, false, false, true, false, false, false]|
+------------------------------------------------------+

--collect_set
 
SELECT COLLECT_SET(c2) FROM buildin_agg;
+---------------+
|collect_set(c2)|
+---------------+
|[1, 2, 3, 4]   |
+---------------+

SELECT COLLECT_SET(c3) FROM buildin_agg;
+------------------------------------------------+
|collect_set(c3)                                 |
+------------------------------------------------+
|[agg7, agg8, agg3, agg6, agg4, agg2, agg5, agg1]|
+------------------------------------------------+

--corr

SELECT CORR(c1, c2) FROM buildin_agg;
+--------------------------------------------+
|corr(CAST(c1 AS DOUBLE), CAST(c2 AS DOUBLE))|
+--------------------------------------------+
|                          0.7745966692414833|
+--------------------------------------------+

--count(*)

SELECT COUNT(*) FROM buildin_agg;
+--------+
|count(1)|
+--------+
|       8|
+--------+

--count

SELECT COUNT(c2) FROM buildin_agg;
+---------+
|count(c2)|
+---------+
|        7|
+---------+

--count distinct

SELECT COUNT(DISTINCT c1) FROM buildin_agg;
+------------------+
|count(DISTINCT c1)|
+------------------+
|                 5|
+------------------+

SELECT COUNT(DISTINCT c1, c2) FROM buildin_agg;
+----------------------+
|count(DISTINCT c1, c2)|
+----------------------+
|                     5|
+----------------------+

--count_if

SELECT COUNT_IF(c1 IS NULL) from buildin_agg;
+----------------------+
|count_if((c1 IS NULL))|
+----------------------+
|                     1|
+----------------------+

SELECT c1 FROM buildin_agg GROUP BY c1 HAVING COUNT_IF(c2 % 2 = 0);
+----+
|  c1|
+----+
|null|
|   1|
+----+

--count_min_sketch

SELECT COUNT_MIN_SKETCH(c1, 1D, 0.2D, 3) FROM buildin_agg;
+----------------------------------------------------------+
|count_min_sketch(c1, 0.9, 0.2, 3)                         |
+----------------------------------------------------------+
|[00 00 00 01 00 00 00 00 00 00 00 07 00 00 00 01 00 00...]|
+----------------------------------------------------------+

--covar_pop

SELECT COVAR_POP(c1, c2) FROM buildin_agg;
+-------------------------------------------------+
|covar_pop(CAST(c1 AS DOUBLE), CAST(c2 AS DOUBLE))|
+-------------------------------------------------+
|                               0.6666666666666666|
+-------------------------------------------------+

--covar_samp

SELECT COVAR_SAMP(c1, c2) FROM buildin_agg;
+--------------------------------------------------+
|covar_samp(CAST(c1 AS DOUBLE), CAST(c2 AS DOUBLE))|
+--------------------------------------------------+
|                                               0.8|
+--------------------------------------------------+

--first and first_value

SELECT FIRST(c1) FROM buildin_agg;
+----------------+
|first(c1, false)|
+----------------+
|               2|
+----------------+

SELECT FIRST(col) FROM VALUES (NULL), (5), (20) AS t(col);
+-----------------+
|first(col, false)|
+-----------------+
|             null|
+-----------------+

SELECT FIRST(col, true) FROM VALUES (NULL), (5), (20) AS t(col);
+----------------+
|first(col, true)|
+----------------+
|               5|
+----------------+

SELECT FIRST_VALUE(col) FROM VALUES (NULL), (5), (20) AS t(col);
+-----------------------+
|first_value(col, false)|
+-----------------------+
|                   null|
+-----------------------+

SELECT FIRST_VALUE(col, true) FROM VALUES (NULL), (5), (20) AS t(col);
+----------------------+
|first_value(col, true)|
+----------------------+
|                     5|
+----------------------+

--kurtosis

SELECT KURTOSIS(c2) FROM buildin_agg;
+----------------------------+
|kurtosis(CAST(c2 AS DOUBLE))|
+----------------------------+
|         -0.7325000000000004|
+----------------------------+

SELECT KURTOSIS(col) FROM VALUES (-1000), (-100), (10), (20) AS t(col);
+-----------------------------+
|kurtosis(CAST(col AS DOUBLE))|
+-----------------------------+
|          -0.7014368047529627|
+-----------------------------+

--last and last_value

SELECT LAST(c1) FROM buildin_agg;
+---------------+
|last(c1, false)|
+---------------+
|           null|
+---------------+

SELECT LAST(c1, true) FROM buildin_agg;
+--------------+
|last(c1, true)|
+--------------+
|             5|
+--------------+

SELECT LAST_VALUE(c1) FROM buildin_agg;
+---------------------+
|last_value(c1, false)|
+---------------------+
|                 null|
+---------------------+

SELECT LAST_VALUE(c1, true) FROM buildin_agg;
+--------------------+
|last_value(c1, true)|
+--------------------+
|                   5|
+--------------------+

--max

SELECT MAX(c2) FROM buildin_agg;
+-------+
|max(c2)|
+-------+
|      4|
+-------+

--max_by

SELECT MAX_BY(c1, c3) FROM buildin_agg;
+-------------+
|maxby(c1, c3)|
+-------------+
|            5|
+-------------+

--min

SELECT MIN(c1) FROM buildin_agg;
+-------+
|min(c1)|
+-------+
|      1|
+-------+

--min_by

SELECT MIN_BY(c2, c3) FROM buildin_agg;
+-------------+
|minby(c2, c3)|
+-------------+
|            1|
+-------------+

--percentile

SELECT PERCENTILE(c1, 0.3) FROM buildin_agg;
+--------------------------------------+
|percentile(c1, CAST(0.3 AS DOUBLE), 1)|
+--------------------------------------+
|                                   1.0|
+--------------------------------------+

SELECT PERCENTILE(c1, 0.3, 2) FROM buildin_agg;
+--------------------------------------+
|percentile(c1, CAST(0.3 AS DOUBLE), 2)|
+--------------------------------------+
|                                   1.0|
+--------------------------------------+

SELECT PERCENTILE(c1, ARRAY(0.25, 0.75)) FROM buildin_agg;
+------------------------------------+
|percentile(c1, array(0.25, 0.75), 1)|
+------------------------------------+
|                          [1.0, 3.5]|
+------------------------------------+

SELECT PERCENTILE(c1, ARRAY(0.25, 0.75), 10) FROM buildin_agg;
+-------------------------------------+
|percentile(c1, array(0.25, 0.75), 10)|
+-------------------------------------+
|                           [1.0, 4.0]|
+-------------------------------------+

--PERCENTILE_APPROX and APPROX_PERCENTILE

SELECT PERCENTILE_APPROX(c1, 0.25, 100) FROM buildin_agg;
+------------------------------------------------+
|percentile_approx(c1, CAST(0.25 AS DOUBLE), 100)|
+------------------------------------------------+
|                                               1|
+------------------------------------------------+

SELECT APPROX_PERCENTILE(c1, 0.25, 100) FROM buildin_agg;
+------------------------------------------------+
|approx_percentile(c1, CAST(0.25 AS DOUBLE), 100)|
+------------------------------------------------+
|                                               1|
+------------------------------------------------+

SELECT PERCENTILE_APPROX(c1, ARRAY(0.25, 0.85), 100) FROM buildin_agg;
+---------------------------------------------+
|percentile_approx(c1, array(0.25, 0.85), 100)|
+---------------------------------------------+
|                                       [1, 4]|
+---------------------------------------------+

SELECT APPROX_PERCENTILE(c1, array(0.25, 0.85), 100) FROM buildin_agg;
+---------------------------------------------+
|approx_percentile(c1, array(0.25, 0.85), 100)|
+---------------------------------------------+
|                                       [1, 4]|
+---------------------------------------------+

--skewness

SELECT SKEWNESS(c1) FROM buildin_agg;
+----------------------------+
|skewness(CAST(c1 AS DOUBLE))|
+----------------------------+
|          0.5200705032248686|
+----------------------------+

SELECT SKEWNESS(col) FROM VALUES (-1000), (-100), (10), (20) AS t(col);
+-----------------------------+
|skewness(CAST(col AS DOUBLE))|
+-----------------------------+
|          -1.1135657469022011|
+-----------------------------+

--stddev_samp, stddev and std

SELECT STDDEV_SAMP(c1) FROM buildin_agg;
+-------------------------------+
|stddev_samp(CAST(c1 AS DOUBLE))|
+-------------------------------+
|              1.618347187425374|
+-------------------------------+

SELECT STDDEV(c1) FROM buildin_agg;
+--------------------------+
|stddev(CAST(c1 AS DOUBLE))|
+--------------------------+
|         1.618347187425374|
+--------------------------+

SELECT STD(c1) FROM buildin_agg;
+-----------------------+
|std(CAST(c1 AS DOUBLE))|
+-----------------------+
|      1.618347187425374|
+-----------------------+

--stddev_pop

SELECT STDDEV_POP(c1) FROM buildin_agg;
+------------------------------+
|stddev_pop(CAST(c1 AS DOUBLE))|
+------------------------------+
|             1.498298354528788|
+------------------------------+

--sum

SELECT SUM(col) FROM VALUES (5), (10), (15) AS t(col);
+--------+
|sum(col)|
+--------+
|      30|
+--------+

SELECT SUM(c1) FROM buildin_agg;
+-------+
|sum(c1)|
+-------+
|     17|
+-------+

SELECT SUM(col) FROM VALUES (NULL), (NULL) AS t(col);
+--------+
|sum(col)|
+--------+
|    null|
+--------+

--variance and var_samp

SELECT VARIANCE(c1) FROM buildin_agg;
+----------------------------+
|variance(CAST(c1 AS DOUBLE))|
+----------------------------+
|           2.619047619047619|
+----------------------------+

SELECT VAR_SAMP(c1) FROM buildin_agg;
+----------------------------+
|var_samp(CAST(c1 AS DOUBLE))|
+----------------------------+
|           2.619047619047619|
+----------------------------+

--var_pop

SELECT VAR_POP(c1) FROM buildin_agg;
+---------------------------+
|var_pop(CAST(c1 AS DOUBLE))|
+---------------------------+
|         2.2448979591836737|
+---------------------------+
{% endhighlight %}