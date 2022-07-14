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

* Table of contents
{:toc}

For some workloads, it is possible to improve performance by either caching data in memory, or by
turning on some experimental options.

## Caching Data In Memory

Spark SQL can cache tables using an in-memory columnar format by calling `spark.catalog.cacheTable("tableName")` or `dataFrame.cache()`.
Then Spark SQL will scan only required columns and will automatically tune compression to minimize
memory usage and GC pressure. You can call `spark.catalog.uncacheTable("tableName")` or `dataFrame.unpersist()` to remove the table from memory.

Configuration of in-memory caching can be done using the `setConf` method on `SparkSession` or by running
`SET key=value` commands using SQL.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
<tr>
  <td><code>spark.sql.inMemoryColumnarStorage.compressed</code></td>
  <td>true</td>
  <td>
    When set to true Spark SQL will automatically select a compression codec for each column based
    on statistics of the data.
  </td>
  <td>1.0.1</td>
</tr>
<tr>
  <td><code>spark.sql.inMemoryColumnarStorage.batchSize</code></td>
  <td>10000</td>
  <td>
    Controls the size of batches for columnar caching. Larger batch sizes can improve memory utilization
    and compression, but risk OOMs when caching data.
  </td>
  <td>1.1.1</td>
</tr>

</table>

## Other Configuration Options

The following options can also be used to tune the performance of query execution. It is possible
that these options will be deprecated in future release as more optimizations are performed automatically.

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
  <tr>
    <td><code>spark.sql.files.maxPartitionBytes</code></td>
    <td>134217728 (128 MB)</td>
    <td>
      The maximum number of bytes to pack into a single partition when reading files.
      This configuration is effective only when using file-based sources such as Parquet, JSON and ORC.
    </td>
    <td>2.0.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.files.openCostInBytes</code></td>
    <td>4194304 (4 MB)</td>
    <td>
      The estimated cost to open a file, measured by the number of bytes could be scanned in the same
      time. This is used when putting multiple files into a partition. It is better to over-estimated,
      then the partitions with small files will be faster than partitions with bigger files (which is
      scheduled first). This configuration is effective only when using file-based sources such as Parquet,
      JSON and ORC.
    </td>
    <td>2.0.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.files.minPartitionNum</code></td>
    <td>Default Parallelism</td>
    <td>
      The suggested (not guaranteed) minimum number of split file partitions. If not set, the default
      value is `spark.default.parallelism`. This configuration is effective only when using file-based
      sources such as Parquet, JSON and ORC.
    </td>
    <td>3.1.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.broadcastTimeout</code></td>
    <td>300</td>
    <td>
      <p>
        Timeout in seconds for the broadcast wait time in broadcast joins
      </p>
    </td>
    <td>1.3.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.autoBroadcastJoinThreshold</code></td>
    <td>10485760 (10 MB)</td>
    <td>
      Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when
      performing a join. By setting this value to -1 broadcasting can be disabled. Note that currently
      statistics are only supported for Hive Metastore tables where the command
      <code>ANALYZE TABLE &lt;tableName&gt; COMPUTE STATISTICS noscan</code> has been run.
    </td>
    <td>1.1.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.shuffle.partitions</code></td>
    <td>200</td>
    <td>
      Configures the number of partitions to use when shuffling data for joins or aggregations.
    </td>
    <td>1.1.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.sources.parallelPartitionDiscovery.threshold</code></td>
    <td>32</td>
    <td>
      Configures the threshold to enable parallel listing for job input paths. If the number of
      input paths is larger than this threshold, Spark will list the files by using Spark distributed job.
      Otherwise, it will fallback to sequential listing. This configuration is only effective when
      using file-based data sources such as Parquet, ORC and JSON.
    </td>
    <td>1.5.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.sources.parallelPartitionDiscovery.parallelism</code></td>
    <td>10000</td>
    <td>
      Configures the maximum listing parallelism for job input paths. In case the number of input
      paths is larger than this value, it will be throttled down to use this value. Same as above,
      this configuration is only effective when using file-based data sources such as Parquet, ORC
      and JSON.
    </td>
    <td>2.1.1</td>
  </tr>
</table>

## Join Strategy Hints for SQL Queries

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

<div data-lang="python"  markdown="1">

{% highlight python %}
spark.table("src").join(spark.table("records").hint("broadcast"), "key").show()
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

## Coalesce Hints for SQL Queries

Coalesce hints allows the Spark SQL users to control the number of output files just like the
`coalesce`, `repartition` and `repartitionByRange` in Dataset API, they can be used for performance
tuning and reducing the number of output files. The "COALESCE" hint only has a partition number as a
parameter. The "REPARTITION" hint has a partition number, columns, or both/neither of them as parameters.
The "REPARTITION_BY_RANGE" hint must have column names and a partition number is optional. The "REBALANCE"
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

## Adaptive Query Execution
Adaptive Query Execution (AQE) is an optimization technique in Spark SQL that makes use of the runtime statistics to choose the most efficient query execution plan, which is enabled by default since Apache Spark 3.2.0. Spark SQL can turn on and off AQE by `spark.sql.adaptive.enabled` as an umbrella configuration. As of Spark 3.0, there are three major features in AQE: including coalescing post-shuffle partitions, converting sort-merge join to broadcast join, and skew join optimization.

### Coalescing Post Shuffle Partitions
This feature coalesces the post shuffle partitions based on the map output statistics when both `spark.sql.adaptive.enabled` and `spark.sql.adaptive.coalescePartitions.enabled` configurations are true. This feature simplifies the tuning of shuffle partition number when running queries. You do not need to set a proper shuffle partition number to fit your dataset. Spark can pick the proper shuffle partition number at runtime once you set a large enough initial number of shuffle partitions via `spark.sql.adaptive.coalescePartitions.initialPartitionNum` configuration.
 <table class="table">
   <tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
   <tr>
     <td><code>spark.sql.adaptive.coalescePartitions.enabled</code></td>
     <td>true</td>
     <td>
       When true and <code>spark.sql.adaptive.enabled</code> is true, Spark will coalesce contiguous shuffle partitions according to the target size (specified by <code>spark.sql.adaptive.advisoryPartitionSizeInBytes</code>), to avoid too many small tasks.
     </td>
     <td>3.0.0</td>
   </tr>
   <tr>
     <td><code>spark.sql.adaptive.coalescePartitions.parallelismFirst</code></td>
     <td>true</td>
     <td>
       When true, Spark ignores the target size specified by <code>spark.sql.adaptive.advisoryPartitionSizeInBytes</code> (default 64MB) when coalescing contiguous shuffle partitions, and only respect the minimum partition size specified by <code>spark.sql.adaptive.coalescePartitions.minPartitionSize</code> (default 1MB), to maximize the parallelism. This is to avoid performance regression when enabling adaptive query execution. It's recommended to set this config to false and respect the target size specified by <code>spark.sql.adaptive.advisoryPartitionSizeInBytes</code>.
     </td>
     <td>3.2.0</td>
   </tr>
   <tr>
     <td><code>spark.sql.adaptive.coalescePartitions.minPartitionSize</code></td>
     <td>1MB</td>
     <td>
       The minimum size of shuffle partitions after coalescing. Its value can be at most 20% of <code>spark.sql.adaptive.advisoryPartitionSizeInBytes</code>. This is useful when the target size is ignored during partition coalescing, which is the default case.
     </td>
     <td>3.2.0</td>
   </tr>
   <tr>
     <td><code>spark.sql.adaptive.coalescePartitions.initialPartitionNum</code></td>
     <td>(none)</td>
     <td>
       The initial number of shuffle partitions before coalescing. If not set, it equals to <code>spark.sql.shuffle.partitions</code>. This configuration only has an effect when <code>spark.sql.adaptive.enabled</code> and <code>spark.sql.adaptive.coalescePartitions.enabled</code> are both enabled.
     </td>
     <td>3.0.0</td>
   </tr>
   <tr>
     <td><code>spark.sql.adaptive.advisoryPartitionSizeInBytes</code></td>
     <td>64 MB</td>
     <td>
       The advisory size in bytes of the shuffle partition during adaptive optimization (when <code>spark.sql.adaptive.enabled</code> is true). It takes effect when Spark coalesces small shuffle partitions or splits skewed shuffle partition.
     </td>
     <td>3.0.0</td>
   </tr>
 </table>
 
### Converting sort-merge join to broadcast join
AQE converts sort-merge join to broadcast hash join when the runtime statistics of any join side is smaller than the adaptive broadcast hash join threshold. This is not as efficient as planning a broadcast hash join in the first place, but it's better than keep doing the sort-merge join, as we can save the sorting of both the join sides, and read shuffle files locally to save network traffic(if `spark.sql.adaptive.localShuffleReader.enabled` is true)
  <table class="table">
     <tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
     <tr>
       <td><code>spark.sql.adaptive.autoBroadcastJoinThreshold</code></td>
       <td>(none)</td>
       <td>
         Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled. The default value is same with <code>spark.sql.autoBroadcastJoinThreshold</code>. Note that, this config is used only in adaptive framework.
       </td>
       <td>3.2.0</td>
     </tr>
  </table>

### Converting sort-merge join to shuffled hash join
AQE converts sort-merge join to shuffled hash join when all post shuffle partitions are smaller than a threshold, the max threshold can see the config `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`.
  <table class="table">
     <tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
     <tr>
       <td><code>spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold</code></td>
       <td>0</td>
       <td>
         Configures the maximum size in bytes per partition that can be allowed to build local hash map. If this value is not smaller than <code>spark.sql.adaptive.advisoryPartitionSizeInBytes</code> and all the partition size are not larger than this config, join selection prefer to use shuffled hash join instead of sort merge join regardless of the value of <code>spark.sql.join.preferSortMergeJoin</code>.
       </td>
       <td>3.2.0</td>
     </tr>
  </table>

### Optimizing Skew Join
Data skew can severely downgrade the performance of join queries. This feature dynamically handles skew in sort-merge join by splitting (and replicating if needed) skewed tasks into roughly evenly sized tasks. It takes effect when both `spark.sql.adaptive.enabled` and `spark.sql.adaptive.skewJoin.enabled` configurations are enabled.
  <table class="table">
     <tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
     <tr>
       <td><code>spark.sql.adaptive.skewJoin.enabled</code></td>
       <td>true</td>
       <td>
         When true and <code>spark.sql.adaptive.enabled</code> is true, Spark dynamically handles skew in sort-merge join by splitting (and replicating if needed) skewed partitions.
       </td>
       <td>3.0.0</td>
     </tr>
     <tr>
       <td><code>spark.sql.adaptive.skewJoin.skewedPartitionFactor</code></td>
       <td>5</td>
       <td>
         A partition is considered as skewed if its size is larger than this factor multiplying the median partition size and also larger than <code>spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes</code>.
       </td>
       <td>3.0.0</td>
     </tr>
     <tr>
       <td><code>spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes</code></td>
       <td>256MB</td>
       <td>
         A partition is considered as skewed if its size in bytes is larger than this threshold and also larger than <code>spark.sql.adaptive.skewJoin.skewedPartitionFactor</code> multiplying the median partition size. Ideally this config should be set larger than <code>spark.sql.adaptive.advisoryPartitionSizeInBytes</code>.
       </td>
       <td>3.0.0</td>
     </tr>
   </table>
