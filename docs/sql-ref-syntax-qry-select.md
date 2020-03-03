---
layout: global
title: SELECT
displayTitle: SELECT 
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
Spark supports a `SELECT` statement and conforms to the ANSI SQL standard. Queries are
used to retrieve result sets from one or more tables. The following section 
describes the overall query syntax and the sub-sections cover different constructs
of a query along with examples. 

### Syntax
{% highlight sql %}
[ WITH with_query [ , ... ] ]
SELECT [ hints , ... ] [ ALL | DISTINCT ] { named_expression [ , ... ] }
  FROM { from_item [ , ...] }
  [ WHERE boolean_expression ]
  [ GROUP BY expression [ , ...] ]
  [ HAVING boolean_expression ]
  [ ORDER BY { expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ...] } ]
  [ SORT  BY { expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ...] } ]
  [ CLUSTER BY { expression [ , ...] } ]
  [ DISTRIBUTE BY { expression [, ...] } ]
  { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select ]
  [ WINDOW { named_window [ , WINDOW named_window, ... ] } ]
  [ LIMIT { ALL | expression } ]
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>with_query</em></code></dt>
  <dd>
    Specifies the common table expressions (CTEs) before the main <code>SELECT</code> query block.
    These table expressions are allowed to be referenced later in the main query. This is useful to abstract
    out repeated subquery blocks in the main query and improves readability of the query.
  </dd>
  <dt><code><em>hints</em></code></dt>
  <dd>
    Hints can be specified to help spark optimizer make better planning decisions. Currently spark supports hints
    that influence selection of join strategies and repartitioning of the data. 
  </dd>
  <dt><code><em>ALL</em></code></dt>
  <dd>
    Select all matching rows from the relation and is enabled by default.
  </dd>
  <dt><code><em>DISTINCT</em></code></dt>
  <dd>
    Select all matching rows from the relation after removing duplicates in results.
  </dd>
  <dt><code><em>named_expression</em></code></dt>
  <dd>
    An expression with an assigned name. In general, it denotes a column expression.<br><br>
    <b>Syntax:</b>
      <code>
        expression [AS] [alias]
      </code>
  </dd>
  <dt><code><em>from_item</em></code></dt>
  <dd>
    Specifies a source of input for the query. It can be one of the following:
    <ol>
      <li>Table relation</li>
      <li>Join relation</li>
      <li>Table valued function</li>
      <li>Inlined table</li>
      <li>Subquery</li>    
    </ol>
  </dd>
  <dt><code><em>WHERE</em></code></dt>
  <dd>
    Filters the result of the FROM clause based on the supplied predicates.
  </dd>
  <dt><code><em>GROUP BY</em></code></dt>
  <dd>
    Specifies the expressions that are used to group the rows. This is used in conjunction with aggregate functions
    (MIN, MAX, COUNT, SUM, AVG) to group rows based on the grouping expressions.
  </dd>
  <dt><code><em>HAVING</em></code></dt>
  <dd>
    Specifies the predicates by which the rows produced by GROUP BY are filtered. The HAVING clause is used to
    filter rows after the grouping is performed.
  </dd>
  <dt><code><em>ORDER BY</em></code></dt>
  <dd>
    Specifies an ordering of the rows of the complete result set of the query. The output rows are ordered
    across the partitions. This parameter is mutually exclusive with <code>SORT BY</code>,
    <code>CLUSTER BY</code> and <code>DISTRIBUTE BY</code> and can not be specified together.
  </dd>
  <dt><code><em>SORT BY</em></code></dt>
  <dd>
    Specifies an ordering by which the rows are ordered within each partition. This parameter is mutually
    exclusive with <code>ORDER BY</code> and <code>CLUSTER BY</code> and can not be specified together.
  </dd>
  <dt><code><em>CLUSTER BY</em></code></dt>
  <dd>
    Specifies a set of expressions that is used to repartition and sort the rows. Using this clause has
    the same effect of using <code>DISTRIBUTE BY</code> and <code>SORT BY</code> together. 
  </dd>
  <dt><code><em>DISTRIBUTE BY</em></code></dt>
  <dd>
    Specifies a set of expressions by which the result rows are repartitioned. This parameter is mutually 
    exclusive with <code>ORDER BY</code> and <code>CLUSTER BY</code> and can not be specified together. 
  </dd>
  <dt><code><em>LIMIT</em></code></dt>
  <dd>
    Specifies the maximum number of rows that can be returned by a statement or subquery. This clause 
    is mostly used in the conjunction with <code>ORDER BY</code> to produce a deterministic result. 
  </dd>
  <dt><code><em>boolean_expression</em></code></dt>
  <dd>
    Specifies an expression with a return type of boolean.
  </dd>
  <dt><code><em>expression</em></code></dt>
  <dd>
    Specifies a combination of one or more values, operators, and SQL functions that evaluates to a value.
  </dd>
  <dt><code><em>named_window</em></code></dt>
  <dd>
    Specifies aliases for one or more source window specifications. The source window specifications can 
    be referenced in the widow definitions in the query.
  </dd>
</dl>

### Related Clauses
- [WHERE Clause](sql-ref-syntax-qry-select-where.html)
- [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
- [HAVING Clause](sql-ref-syntax-qry-select-having.html)
- [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
- [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
- [CLUSTER BY Clause](sql-ref-syntax-qry-select-clusterby.html)
- [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
- [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
