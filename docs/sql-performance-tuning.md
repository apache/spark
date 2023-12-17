---
layout: global
title: Performance Tuning
displayTitle: Performance Tuning
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

Spark offers many techniques for tuning the performance of DataFrame or SQL workloads. Those techniques, broadly speaking, include caching data, altering how datasets are partitioned, selecting the optimal join strategy, and providing the optimizer with additional information it can use to build more efficient execution plans.

* Table of contents
{:toc}

## Caching Data

Spark SQL can cache tables using an in-memory columnar format by calling `spark.catalog.cacheTable("tableName")` or `dataFrame.cache()`.
Then Spark SQL will scan only required columns and will automatically tune compression to minimize
memory usage and GC pressure. You can call `spark.catalog.uncacheTable("tableName")` or `dataFrame.unpersist()` to remove the table from memory.

Configuration of in-memory caching can be done via `spark.conf.set` or by running
`SET key=value` commands using SQL.

{% include_relative _generated/generated-sql-config-table-caching-data.html %}

## Tuning Partitions

{% include_relative _generated/generated-sql-config-table-tuning-partitions.html %}

### Coalesce Hints

Coalesce hints allow Spark SQL users to control the number of output files just like
`coalesce`, `repartition` and `repartitionByRange` in the Dataset API. They can be used for performance
tuning and reducing the number of output files. The `COALESCE` hint only has a partition number as a
parameter. The `REPARTITION` hint has a partition number, columns, or both/neither of them as parameters.
The `REPARTITION_BY_RANGE` hint must have column names and a partition number is optional. The `REBALANCE`
hint has an initial partition number, columns, or both/neither of them as parameters.

```sql
SELECT /*+ COALESCE(3) */ * FROM t;
SELECT /*+ REPARTITION(3) */ * FROM t;
SELECT /*+ REPARTITION(c) */ * FROM t;
SELECT /*+ REPARTITION(3, c) */ * FROM t;
SELECT /*+ REPARTITION */ * FROM t;
SELECT /*+ REPARTITION_BY_RANGE(c) */ * FROM t;
SELECT /*+ REPARTITION_BY_RANGE(3, c) */ * FROM t;
SELECT /*+ REBALANCE */ * FROM t;
SELECT /*+ REBALANCE(3) */ * FROM t;
SELECT /*+ REBALANCE(c) */ * FROM t;
SELECT /*+ REBALANCE(3, c) */ * FROM t;
```

For more details please refer to the documentation of [Partitioning Hints](sql-ref-syntax-qry-select-hints.html#partitioning-hints).

## Optimizing the Join Strategy

### Automatically Broadcasting Joins

{% include_relative _generated/generated-sql-config-table-tuning-broadcast.html %}

### Join Strategy Hints

The join strategy hints, namely `BROADCAST`, `MERGE`, `SHUFFLE_HASH` and `SHUFFLE_REPLICATE_NL`,
instruct Spark to use the hinted strategy on each specified relation when joining them with another
relation. For example, when the `BROADCAST` hint is used on table 't1', broadcast join (either
broadcast hash join or broadcast nested loop join depending on whether there is any equi-join key)
with 't1' as the build side will be prioritized by Spark even if the size of table 't1' suggested
by the statistics is above the configuration `spark.sql.autoBroadcastJoinThreshold`.

When different join strategy hints are specified on both sides of a join, Spark prioritizes the
`BROADCAST` hint over the `MERGE` hint over the `SHUFFLE_HASH` hint over the `SHUFFLE_REPLICATE_NL`
hint. When both sides are specified with the `BROADCAST` hint or the `SHUFFLE_HASH` hint, Spark will
pick the build side based on the join type and the sizes of the relations.

Note that there is no guarantee that Spark will choose the join strategy specified in the hint since
a specific strategy may not support all join types.

<div class="codetabs">
<div data-lang="python"  markdown="1">
{% highlight python %}
spark.table("src").join(spark.table("records").hint("broadcast"), "key").show()
{% endhighlight %}
</div>
<div data-lang="scala"  markdown="1">
{% highlight scala %}
spark.table("src").join(spark.table("records").hint("broadcast"), "key").show()
{% endhighlight %}
</div>
<div data-lang="java"  markdown="1">
{% highlight java %}
spark.table("src").join(spark.table("records").hint("broadcast"), "key").show();
{% endhighlight %}
</div>
<div data-lang="r"  markdown="1">
{% highlight r %}
src <- sql("SELECT * FROM src")
records <- sql("SELECT * FROM records")
head(join(src, hint(records, "broadcast"), src$key == records$key))
{% endhighlight %}
</div>
<div data-lang="SQL"  markdown="1">
{% highlight sql %}
-- We accept BROADCAST, BROADCASTJOIN and MAPJOIN for broadcast hint
SELECT /*+ BROADCAST(r) */ * FROM records r JOIN src s ON r.key = s.key
{% endhighlight %}
</div>
</div>

For more details please refer to the documentation of [Join Hints](sql-ref-syntax-qry-select-hints.html#join-hints).

## Adaptive Query Execution
Adaptive Query Execution (AQE) is an optimization technique in Spark SQL that makes use of the runtime statistics to choose the most efficient query execution plan, which is enabled by default since Apache Spark 3.2.0. Spark SQL can turn on and off AQE by `spark.sql.adaptive.enabled` as an umbrella configuration.

### Coalescing Post Shuffle Partitions
This feature coalesces the post shuffle partitions based on the map output statistics when both `spark.sql.adaptive.enabled` and `spark.sql.adaptive.coalescePartitions.enabled` configurations are true. This feature simplifies the tuning of shuffle partition number when running queries. You do not need to set a proper shuffle partition number to fit your dataset. Spark can pick the proper shuffle partition number at runtime once you set a large enough initial number of shuffle partitions via `spark.sql.adaptive.coalescePartitions.initialPartitionNum` configuration.

{% include_relative _generated/generated-sql-config-table-aqe-coalesce-partitions.html %}

### Splitting skewed shuffle partitions

{% include_relative _generated/generated-sql-config-table-aqe-skewed-shuffle-partitions.html %}

### Converting sort-merge join to broadcast join
AQE converts sort-merge join to broadcast hash join when the runtime statistics of any join side is smaller than the adaptive broadcast hash join threshold. This is not as efficient as planning a broadcast hash join in the first place, but it's better than keep doing the sort-merge join, as we can save the sorting of both the join sides, and read shuffle files locally to save network traffic(if `spark.sql.adaptive.localShuffleReader.enabled` is true)

{% include_relative _generated/generated-sql-config-table-aqe-broadcast-join.html %}

### Converting sort-merge join to shuffled hash join
AQE converts sort-merge join to shuffled hash join when all post shuffle partitions are smaller than the threshold configured in `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`.

{% include_relative _generated/generated-sql-config-table-aqe-shuffled-hash-join.html %}

### Optimizing Skew Join
Data skew can severely downgrade the performance of join queries. This feature dynamically handles skew in sort-merge join by splitting (and replicating if needed) skewed tasks into roughly evenly sized tasks. It takes effect when both `spark.sql.adaptive.enabled` and `spark.sql.adaptive.skewJoin.enabled` configurations are enabled.

{% include_relative _generated/generated-sql-config-table-aqe-skew-join.html %}

### Advanced Customization

You can control the details of how AQE works by providing your own cost evaluator class or by excluding AQE optimizer rules.

{% include_relative _generated/generated-sql-config-table-aqe-advanced.html %}
