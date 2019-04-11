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
    </td>
  </tr>
  <tr>
    <td><code>spark.sql.files.openCostInBytes</code></td>
    <td>4194304 (4 MB)</td>
    <td>
      The estimated cost to open a file, measured by the number of bytes could be scanned in the same
      time. This is used when putting multiple files into a partition. It is better to over-estimated,
      then the partitions with small files will be faster than partitions with bigger files (which is
      scheduled first).
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
import org.apache.spark.sql.functions.broadcast
broadcast(spark.table("src")).join(spark.table("records"), "key").show()
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
import static org.apache.spark.sql.functions.broadcast;
broadcast(spark.table("src")).join(spark.table("records"), "key").show();
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

{% highlight python %}
from pyspark.sql.functions import broadcast
broadcast(spark.table("src")).join(spark.table("records"), "key").show()
{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

{% highlight r %}
src <- sql("SELECT * FROM src")
records <- sql("SELECT * FROM records")
head(join(broadcast(src), records, src$key == records$key))
{% endhighlight %}

</div>

<div data-lang="sql"  markdown="1">

{% highlight sql %}
-- We accept BROADCAST, BROADCASTJOIN and MAPJOIN for broadcast hint
SELECT /*+ BROADCAST(r) */ * FROM records r JOIN src s ON r.key = s.key
{% endhighlight %}

</div>
</div>
