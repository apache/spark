---
layout: global
displayTitle: Structured Streaming Programming Guide
title: Structured Streaming Programming Guide
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

# Real-time Mode

**Real-time Mode** is a new streaming execution mode introduced in Spark 4.1.0 that
targets ultra-low end-to-end latency using the Structured Streaming APIs and processing
guarantees. Some scheduling and callback details differ from micro-batch execution and are
documented below.
It is intended for operational workloads
that must react to data the moment it arrives, such as fraud detection, real-time alerting, and live
personalization.

Real-time Mode in Apache Spark supports **stateless queries** -- projections, filters and other
map-like operations, unions, and stream-static joins -- and, starting in Spark 4.3.0, a first set of
**stateful queries**: streaming **deduplication** (`dropDuplicates`) and streaming **aggregations**
(`groupBy(...).agg(...)`), and the JVM (Scala/Java) **`transformWithState`** operator. These stateful
operations require a shuffle, which Real-time Mode runs as a *pipelined shuffle* so that records
still stream through without waiting for a batch boundary; see
[How Stateful Queries Work](#how-stateful-queries-work). Other stateful operations, including
stream-stream joins and `flatMapGroupsWithState`, are not yet supported. See
[Supported Queries](#supported-queries) for the full list.

The most important thing to know: **the duration you pass to the trigger (default 5 minutes) is
primarily a checkpoint interval, not the latency target for input-driven output.** Records can be
processed and emitted continuously rather than waiting for batch boundaries. Batch-scoped effects,
such as watermark advancement and timers on idle partitions, can still wait for the boundary. See
[Batch Duration Is a Checkpoint Interval](#batch-duration-is-a-checkpoint-interval).

You enable Real-time Mode by setting a Real-time trigger on the streaming write; the rest of your
query is unchanged. See [Enabling Real-time Mode](#enabling-real-time-mode).

## How Real-time Mode Works

By default, Structured Streaming runs a query as a series of small batch jobs -- the *micro-batch*
model. For each micro-batch, the driver plans the batch and launches a fresh set of short-lived
tasks. Those tasks read and process a bounded slice of the input, and the driver commits progress
before planning the next batch. The fixed per-batch planning and task-scheduling overhead places a
floor on end-to-end latency.

Real-time Mode removes this per-batch overhead by launching **long-running tasks** -- one per input
partition. These tasks stay alive for the duration of a (long) batch and process records
continuously as they arrive. Because tasks are scheduled once per batch rather than once per slice
of data, records flow through the operator pipeline (source -> transformations -> sink) without
waiting for a batch boundary. End-to-end latency drops from the ~100 ms micro-batch floor to roughly
the time needed to process and ship one record (often a few milliseconds).

Since records never wait for a batch boundary, the batch duration mainly controls how often the
query checkpoints progress -- as the next section explains.

## How Stateful Queries Work

Stateless operations are per-record: each long-running task reads a partition, transforms records,
and ships them without ever needing data from another partition. Stateful operations are different.
A streaming aggregation, `dropDuplicates`, or `transformWithState` groups records **by key**, so
every record for a given key must reach the same task, no matter which partition it arrived on. In
the micro-batch engine that regrouping is done by a **shuffle**: the batch's producer stage writes
shuffle files, and only once those files are fully materialized does the consumer stage read them
back, grouped by key.

That "materialize, then read" boundary is exactly what Real-time Mode avoids for latency, and a
long-running Real-time task never finishes its batch, so an ordinary shuffle would deadlock -- the
consumer would wait forever for a producer that never completes. Real-time Mode therefore runs the
shuffle differently, using two cooperating pieces:

- **Pipelined shuffle** changes *scheduling*. Normally the scheduler runs the consumer stage only
  after the producer stage has completed. For a Real-time stateful query the producer (the source
  scan) and the consumer (the stateful operator) are marked as a single **pipelined group** and
  scheduled to run **at the same time**, for the whole batch. Neither stage waits for the other to
  finish; they run concurrently as long-running tasks, just like the stateless case.

- **Streaming shuffle** changes *data transport*. Because the two stages run concurrently, shuffle
  data cannot be written to files and read back after the fact. Instead the producer's output is
  streamed directly to the consumer tasks over the network, record by record, so a record is
  regrouped by key and handed to the stateful operator as soon as it is produced -- there is no
  intermediate file and no wait for the batch to end.

Together these let a stateful Real-time query keep the same continuous, per-record flow as a
stateless one: a record is read, routed to the task that owns its key through the streaming shuffle,
merged into state, and emitted -- all without a batch boundary. The regrouped state itself is kept
in Spark's usual state store and checkpointed each batch, so the exactly-once and recovery
guarantees are the same as the micro-batch engine (see [Fault Tolerance](#fault-tolerance)).

This mechanism is enabled automatically in Real-time Mode; there is nothing to configure. It applies
to every shuffle on the streaming path -- the shuffle a stateful operator needs, and also a bare
`repartition` (a hash or round-robin shuffle with no stateful operator), which runs as a pipelined
shuffle in the same way. The one exception is a shuffle that would require a separate preparatory
job: range partitioning (`repartitionByRange`, or an `ORDER BY` that plans to a range shuffle) needs
a sampling job to compute range bounds, and that job cannot complete while the source keeps
producing, so such a query fails to start with
`STREAMING_REAL_TIME_MODE.OPERATOR_OR_SINK_NOT_IN_ALLOWLIST`.

## `transformWithState` in Real-time Mode

Starting in Spark 4.3.0, the JVM `transformWithState` API can run in Real-time Mode. The Scala and
Java APIs are supported; the PySpark `transformWithState` and `transformWithStateInPandas` APIs are
not. `TimeMode.None`, `TimeMode.ProcessingTime`, and `TimeMode.EventTime` are supported. See the
[`transformWithState` guide](./structured-streaming-transform-with-state.html) for the stateful
processor API, state variables, timers, TTL, and initial state. `TimeMode.EventTime` requires an
input event-time watermark declared with `withWatermark`.

The API is the same in micro-batch and Real-time Mode, but the input callback granularity differs.
In micro-batch mode, one `handleInputRows` invocation receives all input rows for a grouping key in
that batch. In Real-time Mode, Spark invokes `handleInputRows` once for each non-late input row, with
a single row in the iterator. A processor used in both modes must therefore work correctly whether
rows for the same key arrive in one invocation or in repeated invocations.

Time-based operations also run incrementally while the long Real-time batch remains open:

- **Processing-time timers** use the current executor clock during the long-running input batch.
  Spark checks for expired timers after each input row reaches the state partition and once more at
  batch completion.
- **Event-time timers** require an input watermark declared with `withWatermark` and use the
  watermark established at the beginning of the batch. That watermark remains fixed for the whole
  batch and advances only between batches. Timers already expired against it are checked after each
  input row and again at batch completion.
- **TTL state** requires `TimeMode.ProcessingTime`. Expired values are not returned when the state
  is accessed. Spark also removes expired values periodically while input rows are processed and
  performs a final cleanup at batch completion.

Timer checks and TTL cleanup are driven by input or batch completion; there is no independent
background polling while a state partition is idle. A processing-time timer on an idle partition
can therefore wait until another row arrives or the current batch completes. Similarly, event-time
watermark progress is bounded by the Real-time batch duration.

When initial state is provided, Spark loads and commits it in a finite bootstrap batch before
starting the first long-running Real-time input batch. Input that is already available waits until
the initial state is durable. The bootstrap uses a regular shuffle; pipelined shuffle begins with
the following input batch. `TimerValues.getCurrentProcessingTimeInMs()` returns the finite bootstrap
batch timestamp while `handleInitialState` is running; it uses the live executor clock after the
long-running input batch starts.

State variables, TTL information, and registered timers are checkpointed and restored after a
restart. A query can resume the same compatible checkpoint in micro-batch or Real-time Mode when it
uses RocksDB and state-store checkpoint format v2; see
[State store defaults](#state-store-defaults).

## Batch Duration Is a Checkpoint Interval

In Real-time Mode, the batch duration is primarily a **checkpoint interval, not the latency
interval for input-driven output.** With the default 5-minute duration, a query can still emit
results produced from input records within milliseconds. The duration controls how often it commits
progress and starts the next long-running batch. It can also bound the delay for batch-scoped work,
including watermark advancement and processing-time timers on idle partitions. This differs from
the micro-batch engine, where all output waits for the batch interval.

Do not confuse the 5-minute default trigger duration with the 5-second minimum allowed duration
described under [Requirements](#requirements): the former is the checkpoint cadence used when you do
not specify a duration, while the latter is the smallest duration you are allowed to set.

Choosing the batch duration is a trade-off:

- A *shorter* batch duration checkpoints more often, giving finer-grained recovery (less work to
  re-process after a failure). However, the query does not process data while it commits progress
  and starts the next batch, so checkpointing too frequently adds more of these gaps, which can
  raise tail (p99) latency, in addition to incurring more planning and commit overhead.
- A *longer* batch duration checkpoints less often, reducing that overhead and those gaps, at the
  cost of coarser-grained recovery (more data re-processed after a failure).

The duration is set on the Real-time trigger, as shown under
[Enabling Real-time Mode](#enabling-real-time-mode).

For stateless Real-time queries, progress is committed using **asynchronous progress tracking**:
the offset and commit logs are written off the record-processing path so that checkpointing does
not stall processing. It is enabled automatically for stateless Real-time queries, and every batch
is checkpointed (the async progress tracking checkpoint interval is fixed at 0 in Real-time Mode).
It can be turned off with the `asyncProgressTrackingEnabled` writer option. Stateful queries,
including `transformWithState`, do not support asynchronous progress tracking and commit progress
synchronously.

## Comparison with Other Modes

The table below summarizes how Real-time Mode relates to the default micro-batch engine and to the
experimental [Continuous Processing](./performance-tips.html#continuous-processing) mode. See
[How Real-time Mode Works](#how-real-time-mode-works) for the mechanism and
[Supported Queries](#supported-queries) for the full list of supported operations.

| Mode | Latency | Processing Guarantees | Supported operations | When to use |
|---|---|---|---|---|
| Micro-batch (default) | ~100 ms | Exactly-once | All streaming operations, including all stateful ones | Stateful or higher-throughput workloads, or queries Real-time Mode does not yet support |
| Real-time Mode | millisecond-scale | Exactly-once | Stateless operations (map-like operations, unions, and stream-static joins) plus stateful deduplication, aggregation, and JVM `transformWithState`; more stateful operations planned | Low-latency workloads |
| Continuous Processing (experimental) | ~1 ms | At-least-once | Map-like only (projections and selections); no stateful operations | Legacy; use Real-time Mode instead |

The **Processing Guarantees** column refers to processing semantics, defined under
[Fault Tolerance](#fault-tolerance); end-to-end delivery additionally depends on the sink and is
independent of the execution mode.

Real-time Mode and Continuous Processing both target millisecond-scale latency, but they differ
substantially:

- **Continuous Processing** (introduced in Spark 2.3) is, and remains, experimental. It supports
  only map-like operations -- projections and selections -- with no stateful operations such as
  aggregations or joins, and it provides at-least-once guarantees. Because it is stateless, the
  exactly-once *processing* guarantee discussed under [Fault Tolerance](#fault-tolerance) does not
  apply to it. These constraints have limited its adoption.
- **Real-time Mode** is designed to support all query shapes, including stateful operations, while
  reusing Spark's mature components such as state management, the Catalyst optimizer, and the
  existing SQL operators. It provides exactly-once processing semantics. It supports stateless
  queries and, starting in Spark 4.3.0, stateful deduplication, aggregation, and JVM
  `transformWithState`; support for the remaining stateful operations is ongoing.

For new low-latency workloads, prefer Real-time Mode over Continuous Processing.

## Enabling Real-time Mode

To run a supported query in Real-time Mode, set a **Real-time trigger** on the streaming write.
Everything else in the query stays the same. For example, the following query reads from a Kafka
topic, applies a stateless transformation, and writes the result to another Kafka topic. Records
flow through with low latency even though the trigger is 5 minutes.

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}
spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "input-topic") \
  .load() \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("topic", "output-topic") \
  .option("checkpointLocation", "/path/to/checkpoint") \
  .outputMode("update") \
  .trigger(realTime="5 minutes") \
  .start()
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.apache.spark.sql.streaming.Trigger

spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "input-topic")
  .load()
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "output-topic")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("update")
  .trigger(Trigger.RealTime("5 minutes"))  // enable Real-time Mode
  .start()
{% endhighlight %}
</div>

<div data-lang="java"  markdown="1">
{% highlight java %}
import org.apache.spark.sql.streaming.Trigger;

spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "input-topic")
  .load()
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "output-topic")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("update")
  .trigger(Trigger.RealTime("5 minutes"))  // enable Real-time Mode
  .start();
{% endhighlight %}
</div>

</div>

### Trigger API

- *Scala and Java*: the trigger is `Trigger.RealTime(...)`, imported from
  `org.apache.spark.sql.streaming.Trigger`. Several forms are available:
  + `Trigger.RealTime()` -- uses the default batch duration of 5 minutes.
  + `Trigger.RealTime("5 minutes")` -- a duration string.
  + `Trigger.RealTime(300000)` -- the batch duration in milliseconds, as a `long`.
  + `Trigger.RealTime(5, TimeUnit.MINUTES)` -- a value together with a
    `java.util.concurrent.TimeUnit`.
  + `Trigger.RealTime(Duration("10 seconds"))` -- a Scala `scala.concurrent.duration.Duration`.
- *Python*: pass the batch duration as a string to the `realTime` keyword argument of `trigger()`,
  for example `.trigger(realTime="5 minutes")`. The duration is required in Python.

### Requirements

A query must satisfy all of the following before it can start in Real-time Mode; each is checked when
the query starts:

- The output mode must be `update`. Any other output mode fails to start with
  `STREAMING_REAL_TIME_MODE.OUTPUT_MODE_NOT_SUPPORTED`.
- A `checkpointLocation` is required, as with any other Structured Streaming query.
- The batch duration must be at least `spark.sql.streaming.realTimeMode.minBatchDuration`
  (5000 ms, i.e. 5 seconds, by default); a shorter interval fails to start with
  `INVALID_STREAMING_REAL_TIME_MODE_TRIGGER_INTERVAL`. The duration string must parse to a positive
  interval, and month-based intervals (for example, `"1 month"`) are not accepted. (This 5-second
  minimum is distinct from the 5-minute default; see
  [Batch Duration Is a Checkpoint Interval](#batch-duration-is-a-checkpoint-interval).)
- Stateful queries do not support asynchronous progress tracking. Do not set the
  `asyncProgressTrackingEnabled` writer option to `true` for a query with a stateful operator.

## Supported Queries

Real-time Mode supports stateless, map-like queries and a first set of stateful queries:
deduplication, aggregation, and JVM `transformWithState`.

The following operations, sources, and sinks are supported:

- *Stateless operations* (supported since Spark 4.1.0):
  + Projections: `select`, `selectExpr`, `withColumn`, `drop`, and the typed `map` / `flatMap` Dataset operations.
  + Selections: `where` / `filter`.
  + Expressions that compile to a projection -- including functions such as `from_json` / `to_json`
    and scalar user-defined functions (UDFs).
  + Column generators such as `explode`.
  + `union` of two or more *distinct* streaming sources. Referencing the same source DataFrame more
    than once is not supported and fails with
    `STREAMING_REAL_TIME_MODE.IDENTICAL_SOURCES_IN_UNION_NOT_SUPPORTED`; create a separate DataFrame
    for each source instead. A union may feed a stateful operator, but a stateful operator cannot
    appear on an input branch before the union; that shape fails with
    `STREAMING_REAL_TIME_MODE.STATEFUL_OPERATORS_BEFORE_UNION_NOT_SUPPORTED`.
  + Stream-static joins, where a streaming DataFrame is joined with a static DataFrame. The static
    side must be broadcast (use the `broadcast(...)` hint), because a stream-static join must not
    introduce a shuffle.

- *Stateful operations* (supported since Spark 4.3.0): these regroup records by key through a
  pipelined shuffle (see [How Stateful Queries Work](#how-stateful-queries-work)) and keep their
  state in the state store, checkpointed each batch.
  + **Deduplication**: `dropDuplicates`. (`dropDuplicatesWithinWatermark` is not yet supported in
    Real-time Mode.)
  + **Streaming aggregation**: `groupBy(...).agg(...)` (and the SQL `GROUP BY` equivalent), including
    windowed aggregations with `window(...)`. Distinct aggregates such as `count(distinct ...)` are
    not supported (see [Not supported](#not-supported)).
  + **JVM `transformWithState`**: the Scala and Java APIs, including value, list, and map state;
    processing-time TTL; processing-time and event-time timers; optional initial state; and output
    event-time columns. See [`transformWithState` in Real-time Mode](#transformwithstate-in-real-time-mode)
    for its incremental execution and timing semantics.
  + `withWatermark` (event-time watermark declaration) is supported and now takes effect: it lets a
    windowed aggregation drop late input and evict the state for windows that have closed, bounding
    how much state a long-running query accumulates. (Real-time Mode always runs in `update` output
    mode -- see [Requirements](#requirements) -- so a windowed aggregation emits each window's
    running result as it changes, and the watermark governs when that window's state is evicted.)

- *Sources*: the source must support Real-time Mode. In Apache Spark, the **Kafka** source supports
  Real-time Mode. An unsupported source fails with
  `STREAMING_REAL_TIME_MODE.INPUT_STREAM_NOT_SUPPORTED`. (The built-in `rate` source is not supported
  as a Real-time source.)

- *Sinks*:
  + **Kafka** sink.
  + **Foreach** sink (via `ForeachWriter`), for writing to arbitrary external systems one record at
    a time. See [Using Foreach](./apis-on-dataframes-and-datasets.html#using-foreach-and-foreachbatch).
    Note that `foreachBatch` is *not* supported, because it processes each batch as a whole rather
    than one record at a time.
  + **Console** and **memory** sinks, which are useful for development and debugging.

  Other sinks fail with `STREAMING_REAL_TIME_MODE.SINK_NOT_SUPPORTED`.

The operators and sinks used by a Real-time query are checked against an allowlist before the query
starts; anything outside the allowlist fails with
`STREAMING_REAL_TIME_MODE.OPERATOR_OR_SINK_NOT_IN_ALLOWLIST`.

### Not supported

The following are not yet supported in Real-time Mode. Unless noted otherwise, a query that uses one
fails to start with `STREAMING_REAL_TIME_MODE.OPERATOR_OR_SINK_NOT_IN_ALLOWLIST`:

- Stateful operations other than those listed above: **stream-stream joins**,
  `flatMapGroupsWithState`, session-window aggregation, and `dropDuplicatesWithinWatermark`.
- The PySpark `transformWithState` and `transformWithStateInPandas` APIs. Real-time Mode currently
  supports only the JVM (Scala/Java) `transformWithState` implementation.
- **Range partitioning**: `repartitionByRange`, or an `ORDER BY` / sort that plans to a range
  shuffle, because computing range bounds needs a separate sampling job that cannot complete while
  the source keeps producing. (A plain `repartition` -- hash or round-robin -- is supported; it runs
  as a pipelined shuffle. See [How Stateful Queries Work](#how-stateful-queries-work).)

**Distinct aggregates** such as `count(distinct ...)` are not supported either, but this is a
general Structured Streaming restriction rather than a Real-time Mode one: any streaming distinct
aggregate is rejected during analysis (with a message suggesting `approx_count_distinct()`),
regardless of the trigger.

Support for more stateful operations is ongoing.

## Fault Tolerance

Real-time Mode provides the same **exactly-once processing** guarantees as the default micro-batch
engine. Two distinct guarantees are worth separating:

- **Exactly-once processing** means every input record's effect on the state the engine manages (for
  example, aggregation counts) is applied effectively once, even across failures and restarts.
- **Delivery semantics** describe whether a record may be written to the external system more than
  once. This is a property of the **sink**, not the execution mode.

Real-time Mode is exactly-once with respect to processing. End-to-end delivery depends on the sink: a
sink that performs idempotent or transactional writes can deliver **exactly-once**, while other sinks
deliver **at-least-once** (duplicates are possible after a failure). The built-in Kafka sink provides
at-least-once delivery, with or without Real-time Mode. Real-time Mode does not yet ship an
exactly-once sink, though one can be implemented.

Internally, offsets are committed at the *end* of each batch, after the corresponding records have
already been written to the sink. If a query fails partway through a batch, it resumes from the last
committed offsets on restart and may re-write records emitted before the failure. Design sinks to
tolerate duplicates -- for example, with idempotent writes -- where exactly-once output matters.

## Examples

The following examples read from Kafka and assume a running Kafka cluster. Most show the same query
in Python, Scala, and Java. The `transformWithState` example is shown in Scala and also applies to
Java; its PySpark APIs are not yet supported in Real-time Mode.

### Stream-static join

Enrich a stream by joining it with a static reference dataset. The static side is wrapped in
`broadcast(...)` so the join is executed as a broadcast (map-side) join, which avoids a shuffle.

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}
from pyspark.sql.functions import broadcast

# Static reference data, read once as a batch DataFrame.
reference = spark.read.format("parquet").load("/path/to/reference")

spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "input-topic") \
  .load() \
  .selectExpr("CAST(key AS STRING) AS joinKey", "CAST(value AS STRING) AS value") \
  .join(broadcast(reference), "joinKey") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("topic", "output-topic") \
  .option("checkpointLocation", "/path/to/checkpoint") \
  .outputMode("update") \
  .trigger(realTime="5 minutes") \
  .start()
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.streaming.Trigger

// Static reference data, read once as a batch DataFrame.
val reference = spark.read.format("parquet").load("/path/to/reference")

spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "input-topic")
  .load()
  .selectExpr("CAST(key AS STRING) AS joinKey", "CAST(value AS STRING) AS value")
  .join(broadcast(reference), "joinKey")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "output-topic")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("update")
  .trigger(Trigger.RealTime("5 minutes"))
  .start()
{% endhighlight %}
</div>

<div data-lang="java"  markdown="1">
{% highlight java %}
import static org.apache.spark.sql.functions.broadcast;
import org.apache.spark.sql.streaming.Trigger;

// Static reference data, read once as a batch DataFrame.
Dataset<Row> reference = spark.read().format("parquet").load("/path/to/reference");

spark
  .readStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "input-topic")
  .load()
  .selectExpr("CAST(key AS STRING) AS joinKey", "CAST(value AS STRING) AS value")
  .join(broadcast(reference), "joinKey")
  .writeStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "output-topic")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("update")
  .trigger(Trigger.RealTime("5 minutes"))
  .start();
{% endhighlight %}
</div>

</div>

### Deduplication

Drop duplicate records by key. This is a stateful operation: Real-time Mode regroups records by the
deduplication key through a pipelined shuffle and keeps the set of seen keys in the state store (see
[How Stateful Queries Work](#how-stateful-queries-work)). No code changes are needed beyond running
under a Real-time trigger.

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}
spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "input-topic") \
  .load() \
  .selectExpr("CAST(key AS STRING) AS id", "CAST(value AS STRING) AS value") \
  .dropDuplicates("id") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("topic", "output-topic") \
  .option("checkpointLocation", "/path/to/checkpoint") \
  .outputMode("update") \
  .trigger(realTime="5 minutes") \
  .start()
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.apache.spark.sql.streaming.Trigger

spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "input-topic")
  .load()
  .selectExpr("CAST(key AS STRING) AS id", "CAST(value AS STRING) AS value")
  .dropDuplicates("id")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "output-topic")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("update")
  .trigger(Trigger.RealTime("5 minutes"))
  .start()
{% endhighlight %}
</div>

<div data-lang="java"  markdown="1">
{% highlight java %}
import org.apache.spark.sql.streaming.Trigger;

spark
  .readStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "input-topic")
  .load()
  .selectExpr("CAST(key AS STRING) AS id", "CAST(value AS STRING) AS value")
  .dropDuplicates("id")
  .writeStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "output-topic")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("update")
  .trigger(Trigger.RealTime("5 minutes"))
  .start();
{% endhighlight %}
</div>

</div>

This keeps every distinct key it has seen in the state store. `dropDuplicatesWithinWatermark`, which
bounds how long keys are retained, is not yet supported in Real-time Mode.

### Streaming aggregation

Maintain a running aggregate per key. Real-time Mode regroups input by the grouping key through a
pipelined shuffle, merges each record into the running aggregate in the state store, and emits the
updated result. Because the output mode is `update`, each key is emitted as it changes rather than
only at the end of the batch.

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}
from pyspark.sql.functions import count

spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "input-topic") \
  .load() \
  .selectExpr("CAST(key AS STRING) AS id") \
  .groupBy("id") \
  .agg(count("*").alias("cnt")) \
  .selectExpr("CAST(id AS STRING) AS key", "CAST(cnt AS STRING) AS value") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("topic", "output-topic") \
  .option("checkpointLocation", "/path/to/checkpoint") \
  .outputMode("update") \
  .trigger(realTime="5 minutes") \
  .start()
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.streaming.Trigger

spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "input-topic")
  .load()
  .selectExpr("CAST(key AS STRING) AS id")
  .groupBy("id")
  .agg(count("*").as("cnt"))
  .selectExpr("CAST(id AS STRING) AS key", "CAST(cnt AS STRING) AS value")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "output-topic")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("update")
  .trigger(Trigger.RealTime("5 minutes"))
  .start()
{% endhighlight %}
</div>

<div data-lang="java"  markdown="1">
{% highlight java %}
import static org.apache.spark.sql.functions.count;
import org.apache.spark.sql.streaming.Trigger;

spark
  .readStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "input-topic")
  .load()
  .selectExpr("CAST(key AS STRING) AS id")
  .groupBy("id")
  .agg(count("*").as("cnt"))
  .selectExpr("CAST(id AS STRING) AS key", "CAST(cnt AS STRING) AS value")
  .writeStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "output-topic")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("update")
  .trigger(Trigger.RealTime("5 minutes"))
  .start();
{% endhighlight %}
</div>

</div>

### JVM `transformWithState`

Run a JVM stateful processor continuously under a Real-time trigger. This processor maintains a
running total for each Kafka key. It sums the iterator so the same implementation works when a
micro-batch invocation contains several rows and when a Real-time invocation contains one row. See
the [`transformWithState` guide](./structured-streaming-transform-with-state.html) for the complete
API. Java applications use the same `TimeMode`, `OutputMode`, and `Trigger.RealTime` settings; the
Java `transformWithState` overload also takes an output encoder.

{% highlight scala %}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.{
  OutputMode, StatefulProcessor, TTLConfig, TimeMode, TimerValues, Trigger, ValueState}

import spark.implicits._

class RunningTotalProcessor
    extends StatefulProcessor[String, (String, Int), String] {
  @transient private var total: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    total = getHandle.getValueState("total", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Int)],
      timerValues: TimerValues): Iterator[String] = {
    val previous = if (total.exists()) total.get() else 0L
    val updated = previous + inputRows.map(_._2.toLong).sum
    total.update(updated)
    Iterator.single(s"$key,$updated")
  }
}

val totals = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "input-topic")
  .load()
  .selectExpr(
    "CAST(key AS STRING) AS id",
    "CAST(CAST(value AS STRING) AS INT) AS delta")
  .as[(String, Int)]
  .groupByKey(_._1)
  .transformWithState(
    statefulProcessor = new RunningTotalProcessor,
    timeMode = TimeMode.ProcessingTime(),
    outputMode = OutputMode.Update())

totals
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "output-topic")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("update")
  .trigger(Trigger.RealTime("5 minutes"))
  .start()
{% endhighlight %}

### Writing to the console for development

The console sink prints output to the driver's standard output and is handy while developing a
query. Note that the console sink buffers each batch's rows and prints them when the batch commits,
so its output appears once per batch -- here, every 30 seconds -- rather than continuously. This
makes it useful for inspecting results, but it does not reflect Real-time Mode's true per-record
latency; to observe that, use a row-by-row sink such as Kafka. A shorter batch duration simply makes
the console refresh more often.

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}
spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "input-topic") \
  .load() \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .where("value IS NOT NULL") \
  .writeStream \
  .format("console") \
  .option("checkpointLocation", "/path/to/checkpoint") \
  .outputMode("update") \
  .trigger(realTime="30 seconds") \
  .start()
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.apache.spark.sql.streaming.Trigger

spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "input-topic")
  .load()
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .where("value IS NOT NULL")
  .writeStream
  .format("console")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("update")
  .trigger(Trigger.RealTime("30 seconds"))
  .start()
{% endhighlight %}
</div>

<div data-lang="java"  markdown="1">
{% highlight java %}
import org.apache.spark.sql.streaming.Trigger;

spark
  .readStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "input-topic")
  .load()
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .where("value IS NOT NULL")
  .writeStream()
  .format("console")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("update")
  .trigger(Trigger.RealTime("30 seconds"))
  .start();
{% endhighlight %}
</div>

</div>

## Configuration

| Configuration | Default | Meaning |
|---|---|---|
| `spark.sql.streaming.realTimeMode.minBatchDuration` | `5000` (ms, 5 seconds) | The minimum batch duration, in milliseconds, allowed for a Real-time trigger. See the batch-duration requirement under [Requirements](#requirements). |
| `spark.sql.streaming.realTimeMode.allowlistCheck` | `true` | Whether to verify that all operators and sinks used by a Real-time query are in the supported allowlist. Disabling this check (not recommended) lets unsupported operators and sinks run at your own risk. |
| `spark.sql.streaming.realTimeMode.dangerouslyAllowCheckpointV1.enabled` | `false` | Whether to allow a Real-time query to use state-store checkpoint format version 1. This is unsafe for stateful queries: format v1 can reuse state-file names when a failed batch is rerun, so the rerun can load stale state and lose updates. Prefer format v2 and a fresh checkpoint location. See [State store defaults](#state-store-defaults). |

### State store defaults

A stateful Real-time query needs a low-latency, recovery-correct state store configuration. Because
that configuration is not the right default for the engine as a whole, Real-time Mode applies it
automatically at query start, only for Real-time queries. These are **soft defaults**: each is set
only when you have not set the config yourself, so an explicit value is preserved -- with the
exception of a few explicit values that are incompatible with Real-time Mode and are rejected at
query start rather than kept (see [Incompatible configurations](#incompatible-configurations)).

| Configuration | Real-time default | Meaning |
|---|---|---|
| `spark.sql.streaming.stateStore.providerClass` | `RocksDBStateStoreProvider` | Real-time Mode defaults to the RocksDB state store, which checkpoint format v2 (below) requires. |
| `spark.sql.streaming.stateStore.checkpointFormatVersion` | `2` | Format v2 gives each batch its own state store checkpoint ids, which is what lets a failed batch be rerun correctly from committed offsets. Real-time Mode requires v2 (see below). |
| `spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled` | `true` | Writes a changelog instead of a full snapshot at each commit, shortening the state-commit step that sits on the critical path between Real-time batches. Applied only when the state store is RocksDB. |
| `spark.sql.execution.sortBeforeRepartition` | `false` | The local sort inserted before a round-robin repartition never drains an unbounded stream and would hang a Real-time query, so Real-time Mode defaults it off. Determinism from the sort is not needed because Real-time Mode does not retry tasks. Like the others this is a soft default -- but an explicit `true` is incompatible, so rather than being kept it is rejected at query start (see [Incompatible configurations](#incompatible-configurations)). |

A Real-time query requires state-store checkpoint format v2. Starting a Real-time query with
format version 1 -- for example, when switching an existing micro-batch query to Real-time Mode, or
when
`spark.sql.streaming.stateStore.checkpointFormatVersion` is pinned to `1` -- fails to start with
`STREAMING_REAL_TIME_MODE.CHECKPOINT_FORMAT_V1_NOT_SUPPORTED`. Use a fresh checkpoint location, or,
accepting the risk of state loss on failure, set
`spark.sql.streaming.realTimeMode.dangerouslyAllowCheckpointV1.enabled=true`.

### Incompatible configurations

The defaults above are applied only when you have not set the config. If you instead set one of the
following to a value that Real-time Mode cannot run with, the query fails to start with
`STREAMING_REAL_TIME_MODE.SQL_CONFIGURATION_NOT_SUPPORTED` rather than having your value silently
overridden:

- `spark.sql.streaming.stateStore.checkpointFormatVersion` set below `2` (unless
  `spark.sql.streaming.realTimeMode.dangerouslyAllowCheckpointV1.enabled=true`).
- `spark.sql.streaming.stateStore.providerClass` set to a provider other than
  `RocksDBStateStoreProvider`.
- `spark.sql.execution.sortBeforeRepartition` set to `true`.

## Best Practices

- Real-time Mode launches long-running tasks -- one per input partition -- that continuously read,
  process, and write data. The number of tasks a query needs depends on how many partitions it reads
  from its sources in parallel. Before starting a Real-time query, ensure the cluster has enough
  cores to run all of these tasks simultaneously and continuously. For example, reading from a Kafka
  topic with 10 partitions requires at least 10 cores for the query to make progress. Real-time Mode
  uses a fixed 1:1 mapping between Kafka topic partitions and reader tasks; the `minPartitions`
  option is not supported in Real-time Mode.
- Stateful queries need cores for **both** stages at once. A stateful Real-time query runs its
  producer stage (the source scan) and its consumer stage (the stateful operator) concurrently as a
  pipelined group (see [How Stateful Queries Work](#how-stateful-queries-work)), and both hold their
  tasks for the whole batch. Size the cluster for the sum: the source's reader tasks plus the
  stateful operator's tasks (`spark.sql.shuffle.partitions` post-shuffle tasks by default). For
  example, a 10-partition Kafka source feeding an aggregation with 5 shuffle partitions needs at
  least 15 cores to make progress.
- Run a single Real-time query per cluster. Because Real-time Mode holds its task slots for the
  entire batch duration, any other queries sharing the cluster compete for the same slots, which can
  starve the Real-time query of resources and increase its latency.

## Caveats

- Real-time Mode provides exactly-once processing semantics, but sinks may receive duplicate records
  after a failure. See [Fault Tolerance](#fault-tolerance) for how to design sinks for exactly-once
  writes.
- Adaptive Query Execution (AQE) is not supported for Real-time Mode queries.
