---
layout: global
title: Hints
displayTitle: Hints
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

Hints give users a way to suggest how Spark SQL to use specific approaches to generate its execution plan.

### Syntax

```sql
/*+ hint [ , ... ] */
```

### Partitioning Hints

Partitioning hints allow users to suggest a partitioning strategy that Spark should follow. `COALESCE`, `REPARTITION`,
and `REPARTITION_BY_RANGE` hints are supported and are equivalent to `coalesce`, `repartition`, and
`repartitionByRange` [Dataset APIs](api/scala/org/apache/spark/sql/Dataset.html), respectively. These hints give users
a way to tune performance and control the number of output files in Spark SQL. When multiple partitioning hints are
specified, multiple nodes are inserted into the logical plan, but the leftmost hint is picked by the optimizer.

#### Partitioning Hints Types

* **COALESCE**

  The `COALESCE` hint can be used to reduce the number of partitions to the specified number of partitions. It takes a partition number as a parameter.

* **REPARTITION**

  The `REPARTITION` hint can be used to repartition to the specified number of partitions using the specified partitioning expressions. It takes a partition number, column names, or both as parameters.

* **REPARTITION_BY_RANGE**

  The `REPARTITION_BY_RANGE` hint can be used to repartition to the specified number of partitions using the specified partitioning expressions. It takes column names and an optional partition number as parameters.

#### Examples

```sql
SELECT /*+ COALESCE(3) */ * FROM t;

SELECT /*+ REPARTITION(3) */ * FROM t;

SELECT /*+ REPARTITION(c) */ * FROM t;

SELECT /*+ REPARTITION(3, c) */ * FROM t;

SELECT /*+ REPARTITION_BY_RANGE(c) */ * FROM t;

SELECT /*+ REPARTITION_BY_RANGE(3, c) */ * FROM t;

-- multiple partitioning hints
EXPLAIN EXTENDED SELECT /*+ REPARTITION(100), COALESCE(500), REPARTITION_BY_RANGE(3, c) */ * FROM t;
== Parsed Logical Plan ==
'UnresolvedHint REPARTITION, [100]
+- 'UnresolvedHint COALESCE, [500]
   +- 'UnresolvedHint REPARTITION_BY_RANGE, [3, 'c]
      +- 'Project [*]
         +- 'UnresolvedRelation [t]

== Analyzed Logical Plan ==
name: string, c: int
Repartition 100, true
+- Repartition 500, false
   +- RepartitionByExpression [c#30 ASC NULLS FIRST], 3
      +- Project [name#29, c#30]
         +- SubqueryAlias spark_catalog.default.t
            +- Relation[name#29,c#30] parquet

== Optimized Logical Plan ==
Repartition 100, true
+- Relation[name#29,c#30] parquet

== Physical Plan ==
Exchange RoundRobinPartitioning(100), false, [id=#121]
+- *(1) ColumnarToRow
   +- FileScan parquet default.t[name#29,c#30] Batched: true, DataFilters: [], Format: Parquet,
      Location: CatalogFileIndex[file:/spark/spark-warehouse/t], PartitionFilters: [],
      PushedFilters: [], ReadSchema: struct<name:string>
```

### Join Hints

Join hints allow users to suggest the join strategy that Spark should use. Prior to Spark 3.0, only the `BROADCAST` Join Hint was supported. `MERGE`, `SHUFFLE_HASH` and `SHUFFLE_REPLICATE_NL` Joint Hints support was added in 3.0. When different join strategy hints are specified on both sides of a join, Spark prioritizes hints in the following order: `BROADCAST` over `MERGE` over `SHUFFLE_HASH` over `SHUFFLE_REPLICATE_NL`. When both sides are specified with the `BROADCAST` hint or the `SHUFFLE_HASH` hint, Spark will pick the build side based on the join type and the sizes of the relations. Since a given strategy may not support all join types, Spark is not guaranteed to use the join strategy suggested by the hint.

#### Join Hints Types

* **BROADCAST**

    Suggests that Spark use broadcast join. The join side with the hint will be broadcast regardless of `autoBroadcastJoinThreshold`. If both sides of the join have the broadcast hints, the one with the smaller size (based on stats) will be broadcast. The aliases for `BROADCAST` are `BROADCASTJOIN` and `MAPJOIN`.

* **MERGE**

    Suggests that Spark use shuffle sort merge join. The aliases for `MERGE` are `SHUFFLE_MERGE` and `MERGEJOIN`.

* **SHUFFLE_HASH**

    Suggests that Spark use shuffle hash join. If both sides have the shuffle hash hints, Spark chooses the smaller side (based on stats) as the build side.

* **SHUFFLE_REPLICATE_NL**

    Suggests that Spark use shuffle-and-replicate nested loop join.

#### Examples

```sql
-- Join Hints for broadcast join
SELECT /*+ BROADCAST(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
SELECT /*+ BROADCASTJOIN (t1) */ * FROM t1 left JOIN t2 ON t1.key = t2.key;
SELECT /*+ MAPJOIN(t2) */ * FROM t1 right JOIN t2 ON t1.key = t2.key;

-- Join Hints for shuffle sort merge join
SELECT /*+ SHUFFLE_MERGE(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
SELECT /*+ MERGEJOIN(t2) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
SELECT /*+ MERGE(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;

-- Join Hints for shuffle hash join
SELECT /*+ SHUFFLE_HASH(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;

-- Join Hints for shuffle-and-replicate nested loop join
SELECT /*+ SHUFFLE_REPLICATE_NL(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;

-- When different join strategy hints are specified on both sides of a join, Spark
-- prioritizes the BROADCAST hint over the MERGE hint over the SHUFFLE_HASH hint
-- over the SHUFFLE_REPLICATE_NL hint.
-- Spark will issue Warning in the following example
-- org.apache.spark.sql.catalyst.analysis.HintErrorLogger: Hint (strategy=merge)
-- is overridden by another hint and will not take effect.
SELECT /*+ BROADCAST(t1), MERGE(t1, t2) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
```

### Related Statements

* [JOIN](sql-ref-syntax-qry-select-join.html)
* [SELECT](sql-ref-syntax-qry-select.html)
