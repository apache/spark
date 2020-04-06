---
layout: global
title: AGGREGATION
displayTitle: AGGREGATION
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


An aggregate function operates on a group of rows and returns a single value.
Spark supports various aggregations.
Some examples of the common aggregation functions are <code>SUM</code>, <code>MIN</code>, <code>MAX</code> and <code>COUNT</code>.
There are a rich set of aggregate functions that can be used along with the
<code>GROUP BY</code> clause in <code>SELECT</code> queries.
Spark also supports advanced aggregations using the <code>CUBE</code>, <code>GROUPING SETS</code> and <code>ROLLUP</code> clauses in <code>GROUP BY</code>.

The following sections describe the query syntax and usage for the different aggregation functions.
* [BUILT-IN AGGREGATE FUNCTIONS](sql-ref-functions-builtin-aggregate.html)
* [CUBE](sql-ref-syntax-qry-select-groupby.html)
* [GROUPING SETS](sql-ref-syntax-qry-select-groupby.html)
* [ROLLUP](sql-ref-syntax-qry-select-groupby.html)
