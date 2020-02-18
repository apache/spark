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
memory usage and GC pressure. You can call `spark.catalog.uncacheTable("tableName")` to remove the table from memory.

Configuration of in-memory caching can be done using the `setConf` method on `SparkSession` or by running
`SET key=value` commands using SQL.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.inMemoryColumnarStorage.compressed</code></td>
  <td>true</td>
  <td>
    When set to true Spark SQL will automatically select a compression codec for each column based
    on statistics of the data.
  </td>
</tr>
<tr>
  <td><code>spark.sql.inMemoryColumnarStorage.batchSize</code></td>
  <td>10000</td>
  <td>
    Controls the size of batches for columnar caching. Larger batch sizes can improve memory utilization
    and compression, but risk OOMs when caching data.
  </td>
</tr>

</table>

## Other Configuration Options

The following options can also be used to tune the performance of query execution. It is possible
that these options will be deprecated in future release as more optimizations are performed automatically.

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td><code>spark.sql.files.maxPartitionBytes</code></td>
    <td>134217728 (128 MB)</td>
    <td>
      The maximum number of bytes to pack into a single partition when reading files.
      This configuration is effective only when using file-based sources such as Parquet, JSON and ORC.
    </td>
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
  </tr>
  <tr>
    <td><code>spark.sql.broadcastTimeout</code></td>
    <td>300</td>
    <td>
    <p>
      Timeout in seconds for the broadcast wait time in broadcast joins
    </p>
    </td>
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
  </tr>
  <tr>
    <td><code>spark.sql.shuffle.partitions</code></td>
    <td>200</td>
    <td>
      Configures the number of partitions to use when shuffling data for joins or aggregations.
    </td>
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

<div data-lang="sql"  markdown="1">

{% highlight sql %}
-- We accept BROADCAST, BROADCASTJOIN and MAPJOIN for broadcast hint
SELECT /*+ BROADCAST(r) */ * FROM records r JOIN src s ON r.key = s.key
{% endhighlight %}

</div>
</div>

## Coalesce Hints for SQL Queries

Coalesce hints allows the Spark SQL users to control the number of output files just like the
`coalesce`, `repartition` and `repartitionByRange` in Dataset API, they can be used for performance
tuning and reducing the number of output files. The "COALESCE" hint only has a partition number as a
parameter. The "REPARTITION" hint has a partition number, columns, or both of them as parameters.
The "REPARTITION_BY_RANGE" hint must have column names and a partition number is optional.

    SELECT /*+ COALESCE(3) */ * FROM t
    SELECT /*+ REPARTITION(3) */ * FROM t
    SELECT /*+ REPARTITION(c) */ * FROM t
    SELECT /*+ REPARTITION(3, c) */ * FROM t
    SELECT /*+ REPARTITION_BY_RANGE(c) */ * FROM t
    SELECT /*+ REPARTITION_BY_RANGE(3, c) */ * FROM t

## Adaptive Query Execution
Adaptive Query Execution (AQE) is an optimization technique in Spark SQL that makes use of the runtime statistics to choose the most efficient query execution plan. AQE is disabled by default. Spark SQL can use the umbrella configuration of `spark.sql.adaptive.enabled` to control whether turn it on/off. As of Spark 3.0, there are three major features in AQE, including coalescing post-shuffle partition number, optimizing local shuffle reader and optimizing skewed join.
 ### Coalescing Post Shuffle Partition Number
 This feature coalesces the post shuffle partitions based on the map output statistics when `spark.sql.adaptive.enabled` and `spark.sql.adaptive.shuffle.reducePostShufflePartitions.enabled` configuration properties are both enabled. There are four following sub-configurations in this optimization rule. And this feature can bring about 1.28x performance gain with query 38 in 3TB TPC-DS.
 <table class="table">
   <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
   <tr>
     <td><code>spark.sql.adaptive.shuffle.reducePostShufflePartitions.enabled</code></td>
     <td>true</td>
     <td>
       When true and <code>spark.sql.adaptive.enabled</code> is enabled, spark will reduce the post shuffle partitions number based on the map output statistics.
     </td>
   </tr>
   <tr>
     <td><code>spark.sql.adaptive.shuffle.minNumPostShufflePartitions</code></td>
     <td>1</td>
     <td>
       The advisory minimum number of post-shuffle partitions used when <code>spark.sql.adaptive.enabled</code> and <code>spark.sql.adaptive.shuffle.reducePostShufflePartitions.enabled</code> are both enabled. It is suggested to be almost 2~3x of the parallelism when doing benchmark.
     </td>
   </tr>
   <tr>
     <td><code>spark.sql.adaptive.shuffle.maxNumPostShufflePartitions</code></td>
     <td>Int.MaxValue</td>
     <td>
       The advisory maximum number of post-shuffle partitions used in adaptive execution. This is used as the initial number of pre-shuffle partitions. By default it equals to <code>spark.sql.shuffle.partitions</code>.
     </td>
   </tr>
   <tr>
     <td><code>spark.sql.adaptive.shuffle.targetPostShuffleInputSize</code></td>
     <td>67108864 (64 MB)</td>
     <td>
       The target post-shuffle input size in bytes of a task when <code>spark.sql.adaptive.enabled</code> and <code>spark.sql.adaptive.shuffle.reducePostShufflePartitions.enabled</code> are both enabled.
     </td>
   </tr>
 </table>
 
 ### Optimize Local Shuffle Reader
 This feature optimize the shuffle reader to local shuffle reader when converting the sort merge join to broadcast hash join in runtime and no additional shuffle introduced. It takes effect when `spark.sql.adaptive.enabled` and `spark.sql.adaptive.shuffle.localShuffleReader.enabled` configuration properties are both enabled. This feature and coalescing post shuffle partition number feature can bring about 1.76x performance gain with query 77 in 3TB TPC-DS.  
 ### Optimize Skewed Join
 This feature choose the skewed partition and creates multi tasks to handle the skewed partition when both enable `spark.sql.adaptive.enabled` and `spark.sql.adaptive.skewedJoinOptimization.enabled`. There are four following sub-configurations in this optimization rule. This feature can bring about 6x performance gain in specific skewed join use case, which can refer the detailed use case in PR#26434.
  <table class="table">
     <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
     <tr>
       <td><code>spark.sql.adaptive.skewedJoinOptimization.enabled</code></td>
       <td>true</td>
       <td>
         When true and <code>spark.sql.adaptive.enabled</code> is enabled, When true and adaptive execution is enabled, a skewed join is automatically optimized at runtime.
       </td>
     </tr>
     <tr>
       <td><code>spark.sql.adaptive.optimizeSkewedJoin.skewedPartitionSizeThreshold</code></td>
       <td>67108864 (64 MB)</td>
       <td>
         Configures the minimum size in bytes for a partition that is considered as a skewed partition in adaptive skewed join.
       </td>
     </tr>
     <tr>
       <td><code>spark.sql.adaptive.optimizeSkewedJoin.skewedPartitionFactor</code></td>
       <td>Int.MaxValue</td>
       <td>
         A partition is considered as a skewed partition if its size is larger than this factor multiple the median partition size and also larger than <code>spark.sql.adaptive.optimizeSkewedJoin.skewedPartitionSizeThreshold</code>
       </td>
     </tr>
     <tr>
       <td><code>spark.sql.adaptive.optimizeSkewedJoin.skewedPartitionMaxSplits</code></td>
       <td>5</td>
       <td>
         Configures the maximum number of task to handle a skewed partition in adaptive skewed join. 
       </td>
     </tr>
   </table>