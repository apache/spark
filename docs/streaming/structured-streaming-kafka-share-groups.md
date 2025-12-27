---
layout: global
title: Structured Streaming + Kafka Share Groups Integration Guide (Kafka broker version 4.0 or higher)
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

Structured Streaming integration for Kafka 4.0+ Share Groups (KIP-932) to read data using queue semantics.

## Overview

Kafka Share Groups enable queue-style consumption where multiple consumers can receive records from the same partition concurrently. Unlike traditional consumer groups with exclusive partition assignment, share groups provide:

- Per-record acknowledgment (ACCEPT, RELEASE, REJECT)
- Automatic redelivery on failure via acquisition locks
- Non-sequential offset tracking

## Linking

    groupId = org.apache.spark
    artifactId = spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}
    version = {{site.SPARK_VERSION_SHORT}}

Requires Kafka client 4.0+ with ShareConsumer API support.

## Reading Data from Kafka Share Groups

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% highlight scala %}
val df = spark
  .readStream
  .format("kafka-share")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("kafka.share.group.id", "my-share-group")
  .option("subscribe", "topic1")
  .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
df = spark \
  .readStream \
  .format("kafka-share") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("kafka.share.group.id", "my-share-group") \
  .option("subscribe", "topic1") \
  .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
Dataset<Row> df = spark
  .readStream()
  .format("kafka-share")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("kafka.share.group.id", "my-share-group")
  .option("subscribe", "topic1")
  .load();

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
{% endhighlight %}
</div>

</div>

## Schema

Each row has the following schema:

| Column | Type |
|--------|------|
| key | binary |
| value | binary |
| topic | string |
| partition | int |
| offset | long |
| timestamp | long |
| timestampType | int |
| headers (optional) | array |

## Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| kafka.bootstrap.servers | yes | none | Kafka broker addresses |
| kafka.share.group.id | yes | none | Share group identifier |
| subscribe | yes | none | Comma-separated list of topics |
| subscribePattern | no | none | Topic pattern (alternative to subscribe) |
| kafka.share.acknowledgment.mode | no | implicit | `implicit` or `explicit` |
| kafka.share.exactly.once.strategy | no | none | `none`, `idempotent`, `two-phase-commit`, or `checkpoint-dedup` |
| kafka.share.parallelism | no | spark.default.parallelism | Number of concurrent consumers |
| kafka.share.lock.timeout.ms | no | 30000 | Acquisition lock timeout |
| includeHeaders | no | false | Include Kafka headers |

## Acknowledgment Modes

### Implicit Mode (Default)

Records are automatically acknowledged as ACCEPT when the batch completes successfully. On failure, acquisition locks expire and Kafka redelivers records.

### Explicit Mode

Use `foreachBatch` to manually acknowledge records:

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
df.writeStream
  .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
    // Process records
    // Acknowledgments handled by user logic
  }
  .start()
{% endhighlight %}
</div>
</div>

## Fault Tolerance

### At-Least-Once Semantics

Default behavior ensures no data loss:

1. Records acquired with time-limited lock (default 30s)
2. On task failure, lock expires automatically
3. Kafka transitions records from ACQUIRED to AVAILABLE
4. Records redelivered to other consumers

### Recovery Scenarios

| Failure | Recovery | Guarantee |
|---------|----------|-----------|
| Task failure | Lock expires, records redelivered | At-least-once |
| Driver failure | Resume from checkpoint | At-least-once |
| Kafka broker failure | WAL replay on new leader | No data loss |

## Exactly-Once Strategies

### Idempotent Sink 

Deduplicate at sink using record coordinates (topic, partition, offset):

{% highlight scala %}
df.writeStream
  .format("delta")
  .option("mergeSchema", "true")
  .start("/path/to/table")
{% endhighlight %}

Works with Delta Lake, JDBC with UPSERT, or any idempotent sink.

### Checkpoint Deduplication

Track processed records in Spark checkpoint:

{% highlight scala %}
spark.readStream
  .format("kafka-share")
  .option("kafka.share.exactly.once.strategy", "checkpoint-dedup")
  ...
{% endhighlight %}

### Two-Phase Commit

Atomic commit of output and acknowledgments. Higher latency but true exactly-once:

{% highlight scala %}
spark.readStream
  .format("kafka-share")
  .option("kafka.share.exactly.once.strategy", "two-phase-commit")
  ...
{% endhighlight %}


## Deploying

Same as traditional Kafka source. Include the `spark-sql-kafka-0-10` artifact and its dependencies when deploying your application.

