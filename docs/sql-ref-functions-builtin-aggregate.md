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

Spark SQL aggregate functions are grouped as <code>agg_funcs</code> in Spark SQL. Below is the list of functions.

**Note:** All functions below have another signature which takes String as a column name instead of Column.

* Table of contents
{:toc}
<table class="table">
  <thead>
    <tr><th style="width:25%">Function</th><th>Parameters</th><th>Description</th></tr>
  </thead>
  <tbody>
    <tr>
      <td> <b>{any | some | bool_or}</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td>Returns true if at least one value is true</td>
    </tr>
    <tr>
      <td> <b>approx_count_distinct</b>(<i>c: Column[, relativeSD: Double]]</i>)</td>
      <td>Column name; relativeSD: the maximum estimation error allowed.</td>
      <td>Returns the estimated cardinality by HyperLogLog++</td>
    </tr>   
    <tr>
      <td> <b>{avg | mean}</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td> Returns the average of values in the input column.</td> 
    </tr>
    <tr>
      <td> <b>{bool_and | every}</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td>Returns true if all values are true</td>
    </tr>
    <tr>
      <td> <b>collect_list</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td>Collects and returns a list of non-unique elements. The function is non-deterministic because the order of collected results depends on the order of the rows which may be non-deterministic after a shuffle</td>
    </tr>       
    <tr>
      <td> <b>collect_set</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td>Collects and returns a set of unique elements. The function is non-deterministic because the order of collected results depends on the order of the rows which may be non-deterministic after a shuffle.</td>
    </tr>
    <tr>
      <td> <b>corr</b>(<i>c1: Column, c2: Column</i>)</td>
      <td>Column name</td>
      <td>Returns Pearson coefficient of correlation between a set of number pairs</td>
    </tr>
    <tr>
      <td> <b>count</b>(<i>*</i>)</td>
      <td>None</td>
      <td>Returns the total number of retrieved rows, including rows containing null</td>
    </tr>
    <tr>
      <td> <b>count</b>(<i>c: Column[, c: Column]</i>)</td>
      <td>Column name</td>
      <td>Returns the number of rows for which the supplied column(s) are all not null</td>
    </tr>
    <tr>
      <td> <b>count</b>(<b>DISTINCT</b> <i> c: Column[, c: Column</i>])</td>
      <td>Column name</td>
      <td>Returns the number of rows for which the supplied column(s) are unique and not null</td>
    </tr> 
    <tr>
      <td> <b>count_if</b>(<i>Predicate</i>)</td>
      <td>Expression that will be used for aggregation calculation</td>
      <td>Returns the count number from the predicate evaluate to <code>TRUE</code> values</td>
    </tr> 
    <tr>
        <td> <b>count_min_sketch</b>(<i>c: Column, eps: double, confidence: double, seed integer</i>)</td>
        <td>Column name; eps is a value between 0.0 and 1.0; confidence is a value between 0.0 and 1.0; seed is a positive integer</td>
        <td>Returns a count-min sketch of a column with the given esp, confidence and seed. The result is an array of bytes, which can be deserialized to a `CountMinSketch` before usage. Count-min sketch is a probabilistic data structure used for cardinality estimation using sub-linear space..</td>
    </tr>
    <tr>
      <td> <b>covar_pop</b>(<i>c1: Column, c2: Column</i>)</td>
      <td>Column name</td>
      <td>Returns the population covariance of a set of number pairs</td>
    </tr> 
    <tr>
      <td> <b>covar_samp</b>(<i>c1: Column, c2: Column</i>)</td>
      <td>Column name</td>
      <td>Returns the sample covariance of a set of number pairs</td>
    </tr>  
    <tr>
      <td> <b>{first | first_value}</b>(<i>c: Column[, isIgnoreNull]</i>)</td>
      <td>Column name[, True/False(default)]</td>
      <td>Returns the first value of column for a group of rows. If <code>isIgnoreNull</code> is true, returns only non-null values, default is false. This function is non-deterministic</td>
    </tr>      
    <tr>
       <td> <b>kurtosis</b>(<i>c: Column</i>)</td>
       <td>Column name</td>
       <td>Returns the kurtosis value calculated from values of a group</td>
    </tr>    
    <tr>
      <td> <b>{last | last_value}</b>(<i>c: Column[, isIgnoreNull]</i>)</td>
      <td>Column name[, True/False(default)]</td>
      <td>Returns the last value of column for a group of rows. If <code>isIgnoreNull</code> is true, returns only non-null values, default is false. This function is non-deterministic</td>
    </tr>      
    <tr>
      <td> <b>max</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td>Returns the maximum value of the column.</td>
    </tr>          
    <tr>
      <td> <b>max_by</b>(<i>c1: Column, c2: Column</i>)</td>
      <td>Column name</td>
      <td>Returns the value of column c1 associated with the maximum value of column c2.</td>
    </tr>   
    <tr>
      <td> <b>min</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td>Returns the minimum value of the column.</td>
    </tr>          
    <tr>
      <td> <b>min_by</b>(<i>c1: Column, c2: Column</i>)</td>
      <td>Column name</td>
      <td>Returns the value of column c1 associated with the minimum value of column c2.</td>
    </tr>      
    <tr>
      <td> <b>percentile</b>(<i>c: Column, percentage [, frequency]</i>)</td>
      <td>Column name; percentage is a number between 0 and 1; frequency is a positive integer</td>
      <td>Returns the exact percentile value of numeric column at the given percentage.</td>
    </tr>         
    <tr>
      <td> <b>percentile</b>(<i>c: Column, <b>array</b>(percentage1 [, percentage2]...) [, frequency]</i>)</td>
      <td>Column name; percentage array is an array of number between 0 and 1; frequency is a positive integer</td>
      <td>Returns the exact percentile value array of numeric column at the given percentage(s).</td>
    </tr>        
    <tr>
      <td> <b>{percentile_approx | percentile_approx}</b>(<i>c: Column, percentage [, frequency]</i>)</td>
      <td>Column name; percentage is a number between 0 and 1; frequency is a positive integer</td>
      <td>Returns the approximate percentile value of numeric column at the given percentage.</td>
    </tr>         
    <tr>
      <td> <b>{percentile_approx | percentile_approx}</b>(<i>c: Column, <b>array</b>(percentage1 [, percentage2]...) [, frequency]</i>)</td>
      <td>Column name; percentage is a number between 0 and 1; frequency is a positive integer</td>
      <td>Returns the approximate percentile value of numeric column at the given percentage.</td>
    </tr>             
    <tr>
       <td> <b>skewness</b>(<i>c: Column</i>)</td>
       <td>Column name</td>
       <td>Returns the skewness value calculated from values of a group</td>
    </tr>    
    <tr>
      <td> <b>{stddev_samp | stddev | std}</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td>Returns the sample standard deviation calculated from values of a group</td>
    </tr>  
    <tr>
      <td> <b>stddev_pop</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td>Returns the population standard deviation calculated from values of a group</td>
    </tr>
    <tr>
      <td> <b>sum</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td>Returns the sum calculated from values of a group.</td>
    </tr>       
    <tr>
      <td> <b>{variance | var_samp}</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td>Returns the sample variance calculated from values of a group</td>
    </tr>    
    <tr>
      <td> <b>var_pop</b>(<i>c: Column</i>)</td>
      <td>Column name</td>
      <td>Returns the population variance calculated from values of a group</td>
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

-- ANY, SOME and BOOL_OR
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

-- APPROX_COUNT_DISTINCT
SELECT APPROX_COUNT_DISTINCT(c1) FROM buildin_agg;
+-------------------------+
|approx_count_distinct(c1)|
+-------------------------+
|                        5|
+-------------------------+

SELECT APPROX_COUNT_DISTINCT(c1,0.39d) FROM buildin_agg;
+-------------------------+
|approx_count_distinct(c1)|
+-------------------------+
|                        6|
+-------------------------+

-- AVG and MEAN functions
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

-- BOOL_AND and EVERY 
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

--COLLECT_LIST
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

--COLLECT_SET
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

-- CORR
SELECT CORR(c1, c2) FROM buildin_agg;
+--------------------------------------------+
|corr(CAST(c1 AS DOUBLE), CAST(c2 AS DOUBLE))|
+--------------------------------------------+
|                          0.7745966692414833|
+--------------------------------------------+

--COUNT(*)
SELECT COUNT(*) FROM buildin_agg;
+--------+
|count(1)|
+--------+
|       8|
+--------+

-- COUNT
SELECT COUNT(c2) FROM buildin_agg;
+---------+
|count(c2)|
+---------+
|        7|
+---------+

--COUNT DISTINCT
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

--COUNT_IF
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

--COUNT_MIN_SKETCH
SELECT COUNT_MIN_SKETCH(c1, 1D, 0.2D, 3) FROM buildin_agg;
+----------------------------------------------------------+
|count_min_sketch(c1, 0.9, 0.2, 3)                         |
+----------------------------------------------------------+
|[00 00 00 01 00 00 00 00 00 00 00 07 00 00 00 01 00 00...]|
+----------------------------------------------------------+

--COVAR_POP
SELECT COVAR_POP(c1, c2) FROM buildin_agg;
+-------------------------------------------------+
|covar_pop(CAST(c1 AS DOUBLE), CAST(c2 AS DOUBLE))|
+-------------------------------------------------+
|                               0.6666666666666666|
+-------------------------------------------------+

--COVAR_SAMP
SELECT COVAR_SAMP(c1, c2) FROM buildin_agg;
+--------------------------------------------------+
|covar_samp(CAST(c1 AS DOUBLE), CAST(c2 AS DOUBLE))|
+--------------------------------------------------+
|                                               0.8|
+--------------------------------------------------+

--FIRST and FIRST_VALUE
SELECT FIRST(c1) FROM buildin_agg;
+----------------+
|first(c1, false)|
+----------------+
|               2|
+----------------+

SELECT FIRST(col) FROM VALUES (NULL), (5), (20) AS TAB(col);
+-----------------+
|first(col, false)|
+-----------------+
|             null|
+-----------------+

SELECT FIRST(col, true) FROM VALUES (NULL), (5), (20) AS TAB(col);
+----------------+
|first(col, true)|
+----------------+
|               5|
+----------------+

SELECT FIRST_VALUE(col) FROM VALUES (NULL), (5), (20) AS TAB(col);
+-----------------------+
|first_value(col, false)|
+-----------------------+
|                   null|
+-----------------------+

SELECT FIRST_VALUE(col, true) FROM VALUES (NULL), (5), (20) AS TAB(col);
+----------------------+
|first_value(col, true)|
+----------------------+
|                     5|
+----------------------+

--KURTOSIS
SELECT KURTOSIS(c2) FROM buildin_agg;
+----------------------------+
|kurtosis(CAST(c2 AS DOUBLE))|
+----------------------------+
|         -0.7325000000000004|
+----------------------------+

SELECT KURTOSIS(col) FROM VALUES (-1000), (-100), (10), (20) AS TAB(col);
+-----------------------------+
|kurtosis(CAST(col AS DOUBLE))|
+-----------------------------+
|          -0.7014368047529627|
+-----------------------------+

--LAST and LAST_VALUE
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

--MAX
SELECT MAX(c2) FROM buildin_agg;
+-------+
|max(c2)|
+-------+
|      4|
+-------+

--MAX_BY
SELECT MAX_BY(c1, c3) FROM buildin_agg;
+-------------+
|maxby(c1, c3)|
+-------------+
|            5|
+-------------+

--MIN
SELECT MIN(c1) FROM buildin_agg;
+-------+
|min(c1)|
+-------+
|      1|
+-------+

--MIN_BY
SELECT MIN_BY(c2, c3) FROM buildin_agg;
+-------------+
|minby(c2, c3)|
+-------------+
|            1|
+-------------+

--PERCENTILE
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

--SKEWNESS
SELECT SKEWNESS(c1) FROM buildin_agg;
+----------------------------+
|skewness(CAST(c1 AS DOUBLE))|
+----------------------------+
|          0.5200705032248686|
+----------------------------+

SELECT SKEWNESS(col) FROM VALUES (-1000), (-100), (10), (20) AS TAB(col);
+-----------------------------+
|skewness(CAST(col AS DOUBLE))|
+-----------------------------+
|          -1.1135657469022011|
+-----------------------------+

--STDDEV_SAMP, STDDEV and STD
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

--STDDEV_POP
SELECT STDDEV_POP(c1) FROM buildin_agg;
+------------------------------+
|stddev_pop(CAST(c1 AS DOUBLE))|
+------------------------------+
|             1.498298354528788|
+------------------------------+

--SUM
SELECT SUM(col) FROM VALUES (5), (10), (15) AS TAB(col);
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

SELECT SUM(col) FROM VALUES (NULL), (NULL) AS TAB(col);
+--------+
|sum(col)|
+--------+
|    null|
+--------+

--VARIANCE and VAR_SAMP
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

--VAR_POP
SELECT VAR_POP(c1) FROM buildin_agg;
+---------------------------+
|var_pop(CAST(c1 AS DOUBLE))|
+---------------------------+
|         2.2448979591836737|
+---------------------------+
{% endhighlight %}