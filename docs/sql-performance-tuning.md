---
layout: global
title: Performance Tuning
displayTitle: Performance Tuning
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

## Broadcast Hint for SQL Queries

The `BROADCAST` hint guides Spark to broadcast each specified table when joining them with another table or view.
When Spark deciding the join methods, the broadcast hash join (i.e., BHJ) is preferred,
even if the statistics is above the configuration `spark.sql.autoBroadcastJoinThreshold`.
When both sides of a join are specified, Spark broadcasts the one having the lower statistics.
Note Spark does not guarantee BHJ is always chosen, since not all cases (e.g. full outer join)
support BHJ. When the broadcast nested loop join is selected, we still respect the hint.

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
