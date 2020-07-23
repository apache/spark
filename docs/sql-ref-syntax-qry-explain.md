---
layout: global
title: EXPLAIN
displayTitle: EXPLAIN
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

The `EXPLAIN` statement is used to provide logical/physical plans for an input statement. 
By default, this clause provides information about a physical plan only.

### Syntax

```sql
EXPLAIN [ EXTENDED | CODEGEN | COST | FORMATTED ] statement
```

### Parameters

* **EXTENDED**

    Generates parsed logical plan, analyzed logical plan, optimized logical plan and physical plan.
    Parsed Logical plan is a unresolved plan that extracted from the query.
    Analyzed logical plans transforms which translates unresolvedAttribute and unresolvedRelation into fully typed objects.
    The optimized logical plan transforms through a set of optimization rules, resulting in the physical plan.

* **CODEGEN**

    Generates code for the statement, if any and a physical plan.

* **COST**

    If plan node statistics are available, generates a logical plan and the statistics.

* **FORMATTED**

    Generates two sections: a physical plan outline and node details.

* **statement**

    Specifies a SQL statement to be explained.

### Examples

```sql
-- Default Output
EXPLAIN select k, sum(v) from values (1, 2), (1, 3) t(k, v) group by k;
+----------------------------------------------------+
|                                                plan|
+----------------------------------------------------+
| == Physical Plan ==
 *(2) HashAggregate(keys=[k#33], functions=[sum(cast(v#34 as bigint))])
 +- Exchange hashpartitioning(k#33, 200), true, [id=#59]
    +- *(1) HashAggregate(keys=[k#33], functions=[partial_sum(cast(v#34 as bigint))])
       +- *(1) LocalTableScan [k#33, v#34]
|
+----------------------------------------------------

-- Using Extended
EXPLAIN EXTENDED select k, sum(v) from values (1, 2), (1, 3) t(k, v) group by k;
+----------------------------------------------------+
|                                                plan|
+----------------------------------------------------+
| == Parsed Logical Plan ==
 'Aggregate ['k], ['k, unresolvedalias('sum('v), None)]
 +- 'SubqueryAlias `t`
    +- 'UnresolvedInlineTable [k, v], [List(1, 2), List(1, 3)]
   
 == Analyzed Logical Plan ==
 k: int, sum(v): bigint
 Aggregate [k#47], [k#47, sum(cast(v#48 as bigint)) AS sum(v)#50L]
 +- SubqueryAlias `t`
    +- LocalRelation [k#47, v#48]
   
 == Optimized Logical Plan ==
 Aggregate [k#47], [k#47, sum(cast(v#48 as bigint)) AS sum(v)#50L]
 +- LocalRelation [k#47, v#48]
   
 == Physical Plan ==
 *(2) HashAggregate(keys=[k#47], functions=[sum(cast(v#48 as bigint))], output=[k#47, sum(v)#50L])
+- Exchange hashpartitioning(k#47, 200), true, [id=#79]
   +- *(1) HashAggregate(keys=[k#47], functions=[partial_sum(cast(v#48 as bigint))], output=[k#47, sum#52L])
    +- *(1) LocalTableScan [k#47, v#48]
|
+----------------------------------------------------+

-- Using Formatted
EXPLAIN FORMATTED select k, sum(v) from values (1, 2), (1, 3) t(k, v) group by k;
+----------------------------------------------------+
|                                                plan|
+----------------------------------------------------+
| == Physical Plan ==
 * HashAggregate (4)
 +- Exchange (3)
    +- * HashAggregate (2)
       +- * LocalTableScan (1)
   
   
 (1) LocalTableScan [codegen id : 1]
 Output: [k#19, v#20]
        
 (2) HashAggregate [codegen id : 1]
 Input: [k#19, v#20]
        
 (3) Exchange
 Input: [k#19, sum#24L]
        
 (4) HashAggregate [codegen id : 2]
 Input: [k#19, sum#24L]
|
+----------------------------------------------------+
```
