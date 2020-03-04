---
layout: global
title: ANSI Compliance
displayTitle: ANSI Compliance
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

Since Spark 3.0, Spark SQL introduces two experimental options to comply with the SQL standard: `spark.sql.ansi.enabled` and `spark.sql.storeAssignmentPolicy` (See a table below for details).

When `spark.sql.ansi.enabled` is set to `true`, Spark SQL follows the standard in basic behaviours (e.g., arithmetic operations, type conversion, and SQL parsing).
Moreover, Spark SQL has an independent option to control implicit casting behaviours when inserting rows in a table.
The casting behaviours are defined as store assignment rules in the standard.

When `spark.sql.storeAssignmentPolicy` is set to `ANSI`, Spark SQL complies with the ANSI store assignment rules. This is a separate configuration because its default value is `ANSI`, while the configuration `spark.sql.ansi.enabled` is disabled by default.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.ansi.enabled</code></td>
  <td>false</td>
  <td>
    (Experimental) When true, Spark tries to conform to the ANSI SQL specification:
    1. Spark will throw a runtime exception if an overflow occurs in any operation on integral/decimal field.
    2. Spark will forbid using the reserved keywords of ANSI SQL as identifiers in the SQL parser.
  </td>
</tr>
<tr>
  <td><code>spark.sql.storeAssignmentPolicy</code></td>
  <td>ANSI</td>
  <td>
    (Experimental) When inserting a value into a column with different data type, Spark will perform type coercion.
    Currently, we support 3 policies for the type coercion rules: ANSI, legacy and strict. With ANSI policy,
    Spark performs the type coercion as per ANSI SQL. In practice, the behavior is mostly the same as PostgreSQL.
    It disallows certain unreasonable type conversions such as converting string to int or double to boolean.
    With legacy policy, Spark allows the type coercion as long as it is a valid Cast, which is very loose.
    e.g. converting string to int or double to boolean is allowed.
    It is also the only behavior in Spark 2.x and it is compatible with Hive.
    With strict policy, Spark doesn't allow any possible precision loss or data truncation in type coercion,
    e.g. converting double to int or decimal to double is not allowed.
  </td>
</tr>
</table>

The following subsections present behaviour changes in arithmetic operations, type conversions, and SQL parsing when the ANSI mode enabled.

### Arithmetic Operations

In Spark SQL, arithmetic operations performed on numeric types (with the exception of decimal) are not checked for overflows by default.
This means that in case an operation causes overflows, the result is the same that the same operation returns in a Java/Scala program (e.g., if the sum of 2 integers is higher than the maximum value representable, the result is a negative number).
On the other hand, Spark SQL returns null for decimal overflows.
When `spark.sql.ansi.enabled` is set to `true` and an overflow occurs in numeric and interval arithmetic operations, it throws an arithmetic exception at runtime.

{% highlight sql %}
-- `spark.sql.ansi.enabled=true`
SELECT 2147483647 + 1;

  java.lang.ArithmeticException: integer overflow

-- `spark.sql.ansi.enabled=false`
SELECT 2147483647 + 1;

  +----------------+
  |(2147483647 + 1)|
  +----------------+
  |     -2147483648|
  +----------------+

{% endhighlight %}

### Type Conversion

Spark SQL has three kinds of type conversions: explicit casting, type coercion, and store assignment casting.
When `spark.sql.ansi.enabled` is set to `true`, explicit casting by `CAST` syntax throws a runtime exception for illegal cast patterns defined in the standard, e.g. casts from a string to an integer.
On the other hand, `INSERT INTO` syntax throws an analysis exception when the ANSI mode enabled via `spark.sql.storeAssignmentPolicy=ANSI`.

Currently, the ANSI mode affects explicit casting and assignment casting only.
In future releases, the behaviour of type coercion might change along with the other two type conversion rules.

{% highlight sql %}
-- Examples of explicit casting

-- `spark.sql.ansi.enabled=true`
SELECT CAST('a' AS INT);

  java.lang.NumberFormatException: invalid input syntax for type numeric: a

SELECT CAST(2147483648L AS INT);

  java.lang.ArithmeticException: Casting 2147483648 to int causes overflow

-- `spark.sql.ansi.enabled=false` (This is a default behaviour)
SELECT CAST('a' AS INT);

  +--------------+
  |CAST(a AS INT)|
  +--------------+
  |          null|
  +--------------+

SELECT CAST(2147483648L AS INT);

  +-----------------------+
  |CAST(2147483648 AS INT)|
  +-----------------------+
  |            -2147483648|
  +-----------------------+

-- Examples of store assignment rules
CREATE TABLE t (v INT);

-- `spark.sql.storeAssignmentPolicy=ANSI`
INSERT INTO t VALUES ('1');

  org.apache.spark.sql.AnalysisException: Cannot write incompatible data to table '`default`.`t`':
  - Cannot safely cast 'v': StringType to IntegerType;

-- `spark.sql.storeAssignmentPolicy=LEGACY` (This is a legacy behaviour until Spark 2.x)
INSERT INTO t VALUES ('1');
SELECT * FROM t;

  +---+
  |  v|
  +---+
  |  1|
  +---+

{% endhighlight %}

### SQL Keywords

When `spark.sql.ansi.enabled` is true, Spark SQL will use the ANSI mode parser.
In this mode, Spark SQL has two kinds of keywords:
* Reserved keywords: Keywords that are reserved and can't be used as identifiers for table, view, column, function, alias, etc.
* Non-reserved keywords: Keywords that have a special meaning only in particular contexts and can be used as identifiers in other contexts. For example, `EXPLAIN SELECT ...` is a command, but EXPLAIN can be used as identifiers in other places.

When the ANSI mode is disabled, Spark SQL has two kinds of keywords:
* Non-reserved keywords: Same definition as the one when the ANSI mode enabled.
* Strict-non-reserved keywords: A strict version of non-reserved keywords, which can not be used as table alias.

By default `spark.sql.ansi.enabled` is false.

Below is a list of all the keywords in Spark SQL.

<table class="table">
  <tr><th rowspan="2" style="vertical-align: middle;"><b>Keyword</b></th><th colspan="2"><b>Spark SQL</b></th><th rowspan="2" style="vertical-align: middle;"><b>SQL-2011</b></th></tr>
  <tr><th><b>ANSI mode</b></th><th><b>default mode</b></th></tr>
  <tr><td>ADD</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>AFTER</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>ALL</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ALTER</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ANALYZE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>AND</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ANTI</td><td>reserved</td><td>strict-non-reserved</td><td>non-reserved</td></tr>
  <tr><td>ANY</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ARCHIVE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>ARRAY</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>AS</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ASC</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>AT</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>AUTHORIZATION</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>BETWEEN</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>BOTH</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>BUCKET</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>BUCKETS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>BY</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>CACHE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>CASCADE</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>CASE</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>CAST</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>CHANGE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>CHECK</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>CLEAR</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>CLUSTER</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>CLUSTERED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>CODEGEN</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>COLLATE</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>COLLECTION</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>COLUMN</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>COLUMNS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>COMMENT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>COMMIT</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>COMPACT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>COMPACTIONS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>COMPUTE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>CONCATENATE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>CONSTRAINT</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>COST</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>CREATE</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>CROSS</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>CUBE</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>CURRENT</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>CURRENT_DATE</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>CURRENT_TIME</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>CURRENT_TIMESTAMP</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>CURRENT_USER</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>DATA</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DATABASE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DATABASES</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DAY</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>DBPROPERTIES</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DEFINED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DELETE</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>DELIMITED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DESC</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DESCRIBE</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>DFS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DIRECTORIES</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DIRECTORY</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DISTINCT</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>DISTRIBUTE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DIV</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>DROP</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ELSE</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>END</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ESCAPE</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ESCAPED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>EXCEPT</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>EXCHANGE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>EXISTS</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>EXPLAIN</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>EXPORT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>EXTENDED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>EXTERNAL</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>EXTRACT</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>FALSE</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>FETCH</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>FIELDS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>FILTER</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>FILEFORMAT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>FIRST</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>FOLLOWING</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>FOR</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>FOREIGN</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>FORMAT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>FORMATTED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>FROM</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>FULL</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>FUNCTION</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>FUNCTIONS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>GLOBAL</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>GRANT</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>GROUP</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>GROUPING</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>HAVING</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>HOUR</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>IF</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>IGNORE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>IMPORT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>IN</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>INDEX</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>INDEXES</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>INNER</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>INPATH</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>INPUTFORMAT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>INSERT</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>INTERSECT</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>INTERVAL</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>INTO</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>IS</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ITEMS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>JOIN</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>KEYS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>LAST</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>LATERAL</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>LAZY</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>LEADING</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>LEFT</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>LIKE</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>LIMIT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>LINES</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>LIST</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>LOAD</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>LOCAL</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>LOCATION</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>LOCK</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>LOCKS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>LOGICAL</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>MACRO</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>MAP</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>MATCHED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>MERGE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>MINUS</td><td>reserved</td><td>strict-non-reserved</td><td>non-reserved</td></tr>
  <tr><td>MINUTE</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>MONTH</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>MSCK</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>NAMESPACE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>NAMESPACES</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>NATURAL</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>NO</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>NOT</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>NULL</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>NULLS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>OF</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ON</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>ONLY</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>OPTION</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>OPTIONS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>OR</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ORDER</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>OUT</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>OUTER</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>OUTPUTFORMAT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>OVER</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>OVERLAPS</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>OVERLAY</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>OVERWRITE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>PARTITION</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>PARTITIONED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>PARTITIONS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>PERCENT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>PIVOT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>PLACING</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>POSITION</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>PRECEDING</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>PRIMARY</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>PRINCIPALS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>PROPERTIES</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>PURGE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>QUERY</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>RANGE</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>RECORDREADER</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>RECORDWRITER</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>RECOVER</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>REDUCE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>REFERENCES</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>REFRESH</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>RENAME</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>REPAIR</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>REPLACE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>RESET</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>RESTRICT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>REVOKE</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>RIGHT</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>RLIKE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>ROLE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>ROLES</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>ROLLBACK</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ROLLUP</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ROW</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>ROWS</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>SCHEMA</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>SECOND</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>SELECT</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>SEMI</td><td>reserved</td><td>strict-non-reserved</td><td>non-reserved</td></tr>
  <tr><td>SEPARATED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>SERDE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>SERDEPROPERTIES</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>SESSION_USER</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>SET</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>SETS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>SHOW</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>SKEWED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>SOME</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>SORT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>SORTED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>START</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>STATISTICS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>STORED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>STRATIFY</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>STRUCT</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>SUBSTR</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>SUBSTRING</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>TABLE</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>TABLES</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>TABLESAMPLE</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>TBLPROPERTIES</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>TEMPORARY</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>TERMINATED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>THEN</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>TO</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>TOUCH</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>TRAILING</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>TRANSACTION</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>TRANSACTIONS</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>TRANSFORM</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>TRIM</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>TRUE</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>TRUNCATE</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>UNARCHIVE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>UNBOUNDED</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>UNCACHE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>UNION</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>UNIQUE</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>UNKNOWN</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>UNLOCK</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>UNSET</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>UPDATE</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>USE</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>USER</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>USING</td><td>reserved</td><td>strict-non-reserved</td><td>reserved</td></tr>
  <tr><td>VALUES</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>VIEW</td><td>non-reserved</td><td>non-reserved</td><td>non-reserved</td></tr>
  <tr><td>WHEN</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>WHERE</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>WINDOW</td><td>non-reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>WITH</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
  <tr><td>YEAR</td><td>reserved</td><td>non-reserved</td><td>reserved</td></tr>
</table>
