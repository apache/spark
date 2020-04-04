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

Aggregate functions
* Table of contents
{:toc}

Spark SQL provides build-in Aggregate functions defines in dataset API and SQL interface. Aggregate functions
operate on a group of rows and return a single value.

Spark SQL Aggregate functions are grouped as "agg_funcs" in spark SQL. Below is the list of functions.

Note: Every below function has another signature which take String as a column name instead of Column.

<table class="table">
  <thead>
    <tr><th>Function</th><th>Parameters(s)</th><<th>Description</th><th></tr>
  </thead>
  <tbody>
    <tr>
      <td>avg(e: Column)</td><td>Column name</td><td> Returns the average of values in the input column.</td> 
    </tr>
    <tr>
      <td>mean(e: Column)</td><td>Column name</td><td> Returns the average of values in the input column.</td> 
    </tr>        
    <tr>
      <td>bool_and(e: Column) every(e: Column)</td><td>Column name</td><td>Returns true if all values are true</td>
    </tr>
    <tr>
      <td>any(e: Column)  some(e: Column) bool_or(e: Column)</td><td>Column name</td><td>Returns true if at least one value is true</td>
    </tr>
    <tr>
      <td>approx_count_distinct(e: Column)</td><td>Column name</td><td>Returns the estimated cardinality by HyperLogLog++</td>td>
    </tr>
    <tr>
      <td>corr(e1: Column, e2: Column)</td><td>Column name</td><td>Returns Pearson coefficient of correlation between a set of number pairs</td>
    </tr>
    <tr>
      <td>count(*)</td><td>None</td><td>Returns the total number of retrieved rows, including rows containing null</td>
    </tr>
    <tr>
      <td>count(e: Column[, e: Column])</td><td>Column name</td><td>Returns the number of rows for which the supplied column(s) are all not null</td>
    </tr>
    <tr>
      <td>count(DISTINCT e: Column[, e: Column])</td><td>Column name</td><td>Returns the number of rows for which the supplied column(s) are unique and non-null</td>
    </tr> 
    <tr>
      <td>count_if(e: Column)</td><td>Column name</td><td>Returns the number of `TRUE` values for the column</td>
    </tr> 
    <tr>
      <td>covar_pop(e1: Column, e2: Column)</td><td>Column name</td><td>Returns the population covariance of a set of number pairs</td>
    </tr> 
    <tr>
      <td>covar_samp(e1: Column, e2: Column)</td><td>Column name</td><td>Returns the sample covariance of a set of number pairs</td>
    </tr>  
    <tr>
      <td>first(e: Column[, isIgnoreNull])</td><td>Column name[, True/False(default)]</td><td>Returns the first value of column for a group of rows.
                                                           If `isIgnoreNull` is true, returns only non-null values, default is false.</td>
    </tr>      
    <tr>
      <td>first_value(e: Column[, isIgnoreNull])</td><td>Column name[, True/False(default)]</td><td>Returns the first value of column for a group of rows.
                                                               If `isIgnoreNull` is true, returns only non-null values, default is false.</td>
    </tr>     
    <tr>
       <td>skewness(e: Column)</td><td>Column name</td><td>Returns the skewness value calculated from values of a group</td>
    </tr>    
    <tr>
       <td>kurtosis(e: Column)</td><td>Column name</td><td>Returns the kurtosis value calculated from values of a group</td>
    </tr>    
    <tr>
      <td>last(e: Column[, isIgnoreNull])</td><td>Column name[, True/False(default)]</td><td>Returns the last value of column for a group of rows.
                                                               If `isIgnoreNull` is true, returns only non-null values, default is false.</td>
    </tr>      
    <tr>
      <td>last_value(e: Column[, isIgnoreNull])</td><td>Column name[, True/False(default)]</td><td>Returns the last value of column for a group of rows.
                                                                   If `isIgnoreNull` is true, returns only non-null values, default is false.</td>
    </tr>     
    <tr>
      <td>max(e: Column)</td><td>Column name</td><td>Returns the maximum value of the column.</td>
    </tr>          
    <tr>
      <td>max_by(e1: Column, e2: Column)</td><td>Column name</td><td>Returns the value of column e1 associated with the maximum value of column e2.</td>
    </tr>   
    <tr>
      <td>min(e: Column)</td><td>Column name</td><td>Returns the minimum value of the column.</td>
    </tr>          
    <tr>
      <td>min_by(e1: Column, e2: Column)</td><td>Column name</td><td>Returns the value of column e1 associated with the minimum value of column e2.</td>
    </tr>      
    <tr>
      <td>percentile(e: Column, percentage [, frequency])</td><td>Column name; percentage is a number between 0 and 1; frequency is a positive integer</td><td>Returns the exact percentile value of numeric column
                       `col` at the given percentage.</td>
    </tr>         
    <tr>
      <td>percentile(e: Column, array(percentage1 [, percentage2]...) [, frequency])</td><td>Column name; percentage array is an array of number between 0 and 1; frequency is a positive integer</td><td>Returns the exact
                                                                           percentile value array of numeric column `col` at the given percentage(s).</td>
    </tr>        
    <tr>
      <td>percentile_approx(e: Column, percentage [, frequency])</td><td>Column name; percentage is a number between 0 and 1; frequency is a positive integer</td><td>Returns the approximate percentile value of numeric
                                                                           column `col` at the given percentage.</td>
    </tr>         
    <tr>
      <td>percentile_approx(e: Column, array(percentage1 [, percentage2]...) [, frequency])</td><td>Column name; percentage array is an array of number between 0 and 1; frequency is a positive integer</td><td>Returns the approximate
                                                                           percentile value array of numeric column `col` at the given percentage(s).</td>
    </tr>      
    <tr>
      <td>approx_percentile(e: Column, percentage [, frequency])</td><td>Column name; percentage is a number between 0 and 1; frequency is a positive integer</td><td>Returns the approximate percentile value of numeric
                                                                           column `col` at the given percentage.</td>
    </tr>         
    <tr>
      <td>approx_percentile(e: Column, array(percentage1 [, percentage2]...) [, frequency])</td><td>Column name; percentage array is an array of number between 0 and 1; frequency is a positive integer</td><td>Returns the approximate
                                                                           percentile value array of numeric column `col` at the given percentage(s).</td>
    </tr>        
    <tr>
      <td>stddev_samp(e: Column)</td><td>Column name</td><td>Returns the sample standard deviation calculated from values of a group</td>
    </tr>  
    <tr>
      <td>stddev(e: Column)</td><td>Column name</td><td>Returns the sample standard deviation calculated from values of a group</td>
    </tr>  
    <tr>
      <td>std(e: Column)</td><td>Column name</td><td>Returns the sample standard deviation calculated from values of a group</td>
    </tr>  
    <tr>
      <td>stddev_pop(e: Column)</td><td>Column name</td><td>Returns the population standard deviation calculated from values of a group</td>
    </tr>
    <tr>
      <td>stddev_samp(e: Column)</td><td>Column name</td><td>Returns the sum calculated from values of a group</td>
    </tr>    
    <tr>
      <td>(variance | var_samp)(e: Column)</td><td>Column name</td><td>Returns the sample variance calculated from values of a group</td>
    </tr>    
    <tr>
      <td>sum(e: Column)</td><td>Column name</td><td>Returns the sum calculated from values of a group.</td>
    </tr>       
    <tr>
      <td>var_pop(e: Column)</td><td>Column name</td><td>Returns the population variance calculated from values of a group</td>
    </tr>        
    <tr>
      <td>collect_list(e: Column)</td><td>Column name</td><td>Collects and returns a list of non-unique elements. The function is non-deterministic because the order of collected results depends
                                                          on the order of the rows which may be non-deterministic after a shuffle</td>
    </tr>       
    <tr>
      <td>collect_set(e: Column)</td><td>Column name</td><td>Collects and returns a set of unique elements. The function is non-deterministic because the order of collected results depends
                                                         on the order of the rows which may be non-deterministic after a shuffle.</td>
    </tr>
    <tr>
        <td>count_min_sketch(e: Column, eps: double, confidence: double, seed integer)</td><td>Column name; eps is a value between 0.0 and 1.0; confidence is a value between 0.0 and 1.0; seed is a positive integer</td><td>Returns a count-min sketch of a column with the given esp,
                                                        confidence and seed. The result is an array of bytes, which can be deserialized to a `CountMinSketch` before usage. Count-min sketch is a probabilistic data structure used for
                                                        cardinality estimation using sub-linear space..</td>
    </tr>
                              
  </tbody>
</table>

