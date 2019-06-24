---
layout: global
title: Structured Streaming + Kafka Integration Guide (Kafka broker version 0.10.0 or higher)
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

Structured Streaming integration for Kafka 0.10 to read data from and write data to Kafka.

## Linking
For Scala/Java applications using SBT/Maven project definitions, link your application with the following artifact:

    groupId = org.apache.spark
    artifactId = spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}
    version = {{site.SPARK_VERSION_SHORT}}

For Python applications, you need to add this above library and its dependencies when deploying your
application. See the [Deploying](#deploying) subsection below.

For experimenting on `spark-shell`, you need to add this above library and its dependencies too when invoking `spark-shell`. Also, see the [Deploying](#deploying) subsection below.

## Reading Data from Kafka

### Creating a Kafka Source for Streaming Queries

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}

// Subscribe to 1 topic
val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to multiple topics
val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1,topic2")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to a pattern
val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribePattern", "topic.*")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}

// Subscribe to 1 topic
Dataset<Row> df = spark
  .readStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .load();
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

// Subscribe to multiple topics
Dataset<Row> df = spark
  .readStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1,topic2")
  .load();
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

// Subscribe to a pattern
Dataset<Row> df = spark
  .readStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribePattern", "topic.*")
  .load();
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}

# Subscribe to 1 topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Subscribe to multiple topics
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1,topic2") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Subscribe to a pattern
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribePattern", "topic.*") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

{% endhighlight %}
</div>
</div>

### Creating a Kafka Source for Batch Queries
If you have a use case that is better suited to batch processing,
you can create a Dataset/DataFrame for a defined range of offsets.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}

// Subscribe to 1 topic defaults to the earliest and latest offsets
val df = spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to multiple topics, specifying explicit Kafka offsets
val df = spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1,topic2")
  .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
  .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to a pattern, at the earliest and latest offsets
val df = spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribePattern", "topic.*")
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}

// Subscribe to 1 topic defaults to the earliest and latest offsets
Dataset<Row> df = spark
  .read()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .load();
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

// Subscribe to multiple topics, specifying explicit Kafka offsets
Dataset<Row> df = spark
  .read()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1,topic2")
  .option("startingOffsets", "{\"topic1\":{\"0\":23,\"1\":-2},\"topic2\":{\"0\":-2}}")
  .option("endingOffsets", "{\"topic1\":{\"0\":50,\"1\":-1},\"topic2\":{\"0\":-1}}")
  .load();
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

// Subscribe to a pattern, at the earliest and latest offsets
Dataset<Row> df = spark
  .read()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribePattern", "topic.*")
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
  .load();
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}

# Subscribe to 1 topic defaults to the earliest and latest offsets
df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Subscribe to multiple topics, specifying explicit Kafka offsets
df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1,topic2") \
  .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""") \
  .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Subscribe to a pattern, at the earliest and latest offsets
df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribePattern", "topic.*") \
  .option("startingOffsets", "earliest") \
  .option("endingOffsets", "latest") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
{% endhighlight %}
</div>
</div>

Each row in the source has the following schema:
<table class="table">
<tr><th>Column</th><th>Type</th></tr>
<tr>
  <td>key</td>
  <td>binary</td>
</tr>
<tr>
  <td>value</td>
  <td>binary</td>
</tr>
<tr>
  <td>topic</td>
  <td>string</td>
</tr>
<tr>
  <td>partition</td>
  <td>int</td>
</tr>
<tr>
  <td>offset</td>
  <td>long</td>
</tr>
<tr>
  <td>timestamp</td>
  <td>timestamp</td>
</tr>
<tr>
  <td>timestampType</td>
  <td>int</td>
</tr>
</table>

The following options must be set for the Kafka source
for both batch and streaming queries.

<table class="table">
<tr><th>Option</th><th>value</th><th>meaning</th></tr>
<tr>
  <td>assign</td>
  <td>json string {"topicA":[0,1],"topicB":[2,4]}</td>
  <td>Specific TopicPartitions to consume.
  Only one of "assign", "subscribe" or "subscribePattern"
  options can be specified for Kafka source.</td>
</tr>
<tr>
  <td>subscribe</td>
  <td>A comma-separated list of topics</td>
  <td>The topic list to subscribe.
  Only one of "assign", "subscribe" or "subscribePattern"
  options can be specified for Kafka source.</td>
</tr>
<tr>
  <td>subscribePattern</td>
  <td>Java regex string</td>
  <td>The pattern used to subscribe to topic(s).
  Only one of "assign, "subscribe" or "subscribePattern"
  options can be specified for Kafka source.</td>
</tr>
<tr>
  <td>kafka.bootstrap.servers</td>
  <td>A comma-separated list of host:port</td>
  <td>The Kafka "bootstrap.servers" configuration.</td>
</tr>
</table>

The following configurations are optional:

<table class="table">
<tr><th>Option</th><th>value</th><th>default</th><th>query type</th><th>meaning</th></tr>
<tr>
  <td>startingOffsets</td>
  <td>"earliest", "latest" (streaming only), or json string
  """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
  </td>
  <td>"latest" for streaming, "earliest" for batch</td>
  <td>streaming and batch</td>
  <td>The start point when a query is started, either "earliest" which is from the earliest offsets,
  "latest" which is just from the latest offsets, or a json string specifying a starting offset for
  each TopicPartition.  In the json, -2 as an offset can be used to refer to earliest, -1 to latest.
  Note: For batch queries, latest (either implicitly or by using -1 in json) is not allowed.
  For streaming queries, this only applies when a new query is started, and that resuming will
  always pick up from where the query left off. Newly discovered partitions during a query will start at
  earliest.</td>
</tr>
<tr>
  <td>endingOffsets</td>
  <td>latest or json string
  {"topicA":{"0":23,"1":-1},"topicB":{"0":-1}}
  </td>
  <td>latest</td>
  <td>batch query</td>
  <td>The end point when a batch query is ended, either "latest" which is just referred to the
  latest, or a json string specifying an ending offset for each TopicPartition.  In the json, -1
  as an offset can be used to refer to latest, and -2 (earliest) as an offset is not allowed.</td>
</tr>
<tr>
  <td>failOnDataLoss</td>
  <td>true or false</td>
  <td>true</td>
  <td>streaming and batch</td>
  <td>Whether to fail the query when it's possible that data is lost (e.g., topics are deleted, or
  offsets are out of range). This may be a false alarm. You can disable it when it doesn't work
  as you expected.</td>
</tr>
<tr>
  <td>kafkaConsumer.pollTimeoutMs</td>
  <td>long</td>
  <td>512</td>
  <td>streaming and batch</td>
  <td>The timeout in milliseconds to poll data from Kafka in executors.</td>
</tr>
<tr>
  <td>fetchOffset.numRetries</td>
  <td>int</td>
  <td>3</td>
  <td>streaming and batch</td>
  <td>Number of times to retry before giving up fetching Kafka offsets.</td>
</tr>
<tr>
  <td>fetchOffset.retryIntervalMs</td>
  <td>long</td>
  <td>10</td>
  <td>streaming and batch</td>
  <td>milliseconds to wait before retrying to fetch Kafka offsets</td>
</tr>
<tr>
  <td>maxOffsetsPerTrigger</td>
  <td>long</td>
  <td>none</td>
  <td>streaming and batch</td>
  <td>Rate limit on maximum number of offsets processed per trigger interval. The specified total number of offsets will be proportionally split across topicPartitions of different volume.</td>
</tr>
<tr>
  <td>groupIdPrefix</td>
  <td>string</td>
  <td>spark-kafka-source</td>
  <td>streaming and batch</td>
  <td>Prefix of consumer group identifiers (`group.id`) that are generated by structured streaming
  queries. If "kafka.group.id" is set, this option will be ignored. </td>
</tr>
<tr>
  <td>kafka.group.id</td>
  <td>string</td>
  <td>none</td>
  <td>streaming and batch</td>
  <td>The Kafka group id to use in Kafka consumer while reading from Kafka. Use this with caution.
  By default, each query generates a unique group id for reading data. This ensures that each Kafka
  source has its own consumer group that does not face interference from any other consumer, and
  therefore can read all of the partitions of its subscribed topics. In some scenarios (for example,
  Kafka group-based authorization), you may want to use a specific authorized group id to read data.
  You can optionally set the group id. However, do this with extreme caution as it can cause
  unexpected behavior. Concurrently running queries (both, batch and streaming) or sources with the
  same group id are likely interfere with each other causing each query to read only part of the
  data. This may also occur when queries are started/restarted in quick succession. To minimize such
  issues, set the Kafka consumer session timeout (by setting option "kafka.session.timeout.ms") to
  be very small. When this is set, option "groupIdPrefix" will be ignored. </td>
</tr>
</table>

### Consumer Caching

It's time-consuming to initialize Kafka consumers, especially in streaming scenarios where processing time is a key factor.
Because of this, Spark caches Kafka consumers on executors. The caching key is built up from the following information:
* Topic name
* Topic partition
* Group ID

The size of the cache is limited by <code>spark.kafka.consumer.cache.capacity</code> (default: 64).
If this threshold is reached, it tries to remove the least-used entry that is currently not in use.
If it cannot be removed, then the cache will keep growing. In the worst case, the cache will grow to
the max number of concurrent tasks that can run in the executor (that is, number of tasks slots),
after which it will never reduce.

If a task fails for any reason the new task is executed with a newly created Kafka consumer for safety reasons.
At the same time the cached Kafka consumer which was used in the failed execution will be invalidated. Here it has to
be emphasized it will not be closed if any other task is using it.

## Writing Data to Kafka

Here, we describe the support for writing Streaming Queries and Batch Queries to Apache Kafka. Take note that
Apache Kafka only supports at least once write semantics. Consequently, when writing---either Streaming Queries
or Batch Queries---to Kafka, some records may be duplicated; this can happen, for example, if Kafka needs
to retry a message that was not acknowledged by a Broker, even though that Broker received and wrote the message record.
Structured Streaming cannot prevent such duplicates from occurring due to these Kafka write semantics. However,
if writing the query is successful, then you can assume that the query output was written at least once. A possible
solution to remove duplicates when reading the written data could be to introduce a primary (unique) key
that can be used to perform de-duplication when reading.

The Dataframe being written to Kafka should have the following columns in schema:
<table class="table">
<tr><th>Column</th><th>Type</th></tr>
<tr>
  <td>key (optional)</td>
  <td>string or binary</td>
</tr>
<tr>
  <td>value (required)</td>
  <td>string or binary</td>
</tr>
<tr>
  <td>topic (*optional)</td>
  <td>string</td>
</tr>
</table>
\* The topic column is required if the "topic" configuration option is not specified.<br>

The value column is the only required option. If a key column is not specified then
a ```null``` valued key column will be automatically added (see Kafka semantics on
how ```null``` valued key values are handled). If a topic column exists then its value
is used as the topic when writing the given row to Kafka, unless the "topic" configuration
option is set i.e., the "topic" configuration option overrides the topic column.

The following options must be set for the Kafka sink
for both batch and streaming queries.

<table class="table">
<tr><th>Option</th><th>value</th><th>meaning</th></tr>
<tr>
  <td>kafka.bootstrap.servers</td>
  <td>A comma-separated list of host:port</td>
  <td>The Kafka "bootstrap.servers" configuration.</td>
</tr>
</table>

The following configurations are optional:

<table class="table">
<tr><th>Option</th><th>value</th><th>default</th><th>query type</th><th>meaning</th></tr>
<tr>
  <td>topic</td>
  <td>string</td>
  <td>none</td>
  <td>streaming and batch</td>
  <td>Sets the topic that all rows will be written to in Kafka. This option overrides any
  topic column that may exist in the data.</td>
</tr>
</table>

### Creating a Kafka Sink for Streaming Queries

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}

// Write key-value data from a DataFrame to a specific Kafka topic specified in an option
val ds = df
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "topic1")
  .start()

// Write key-value data from a DataFrame to Kafka using a topic specified in the data
val ds = df
  .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .start()

{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}

// Write key-value data from a DataFrame to a specific Kafka topic specified in an option
StreamingQuery ds = df
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "topic1")
  .start();

// Write key-value data from a DataFrame to Kafka using a topic specified in the data
StreamingQuery ds = df
  .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .start();

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}

# Write key-value data from a DataFrame to a specific Kafka topic specified in an option
ds = df \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("topic", "topic1") \
  .start()

# Write key-value data from a DataFrame to Kafka using a topic specified in the data
ds = df \
  .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .start()

{% endhighlight %}
</div>
</div>

### Writing the output of Batch Queries to Kafka

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}

// Write key-value data from a DataFrame to a specific Kafka topic specified in an option
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .write
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "topic1")
  .save()

// Write key-value data from a DataFrame to Kafka using a topic specified in the data
df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
  .write
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .save()

{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}

// Write key-value data from a DataFrame to a specific Kafka topic specified in an option
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .write()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "topic1")
  .save();

// Write key-value data from a DataFrame to Kafka using a topic specified in the data
df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
  .write()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .save();

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}

# Write key-value data from a DataFrame to a specific Kafka topic specified in an option
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("topic", "topic1") \
  .save()

# Write key-value data from a DataFrame to Kafka using a topic specified in the data
df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .save()

{% endhighlight %}
</div>
</div>


## Kafka Specific Configurations

Kafka's own configurations can be set via `DataStreamReader.option` with `kafka.` prefix, e.g,
`stream.option("kafka.bootstrap.servers", "host:port")`. For possible kafka parameters, see
[Kafka consumer config docs](http://kafka.apache.org/documentation.html#newconsumerconfigs) for
parameters related to reading data, and [Kafka producer config docs](http://kafka.apache.org/documentation/#producerconfigs)
for parameters related to writing data.

Note that the following Kafka params cannot be set and the Kafka source or sink will throw an exception:

- **group.id**: Kafka source will create a unique group id for each query automatically. The user can
set the prefix of the automatically generated group.id's via the optional source option `groupIdPrefix`,
default value is "spark-kafka-source". You can also set "kafka.group.id" to force Spark to use a special
group id, however, please read warnings for this option and use it with caution.
- **auto.offset.reset**: Set the source option `startingOffsets` to specify
 where to start instead. Structured Streaming manages which offsets are consumed internally, rather
 than rely on the kafka Consumer to do it. This will ensure that no data is missed when new
 topics/partitions are dynamically subscribed. Note that `startingOffsets` only applies when a new
 streaming query is started, and that resuming will always pick up from where the query left off.
- **key.deserializer**: Keys are always deserialized as byte arrays with ByteArrayDeserializer. Use
 DataFrame operations to explicitly deserialize the keys.
- **value.deserializer**: Values are always deserialized as byte arrays with ByteArrayDeserializer.
 Use DataFrame operations to explicitly deserialize the values.
- **key.serializer**: Keys are always serialized with ByteArraySerializer or StringSerializer. Use
DataFrame operations to explicitly serialize the keys into either strings or byte arrays.
- **value.serializer**: values are always serialized with ByteArraySerializer or StringSerializer. Use
DataFrame operations to explicitly serialize the values into either strings or byte arrays.
- **enable.auto.commit**: Kafka source doesn't commit any offset.
- **interceptor.classes**: Kafka source always read keys and values as byte arrays. It's not safe to
 use ConsumerInterceptor as it may break the query.

## Deploying

As with any Spark applications, `spark-submit` is used to launch your application. `spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}`
and its dependencies can be directly added to `spark-submit` using `--packages`, such as,

    ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} ...

For experimenting on `spark-shell`, you can also use `--packages` to add `spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}` and its dependencies directly,

    ./bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} ...

See [Application Submission Guide](submitting-applications.html) for more details about submitting
applications with external dependencies.

## Security

Kafka 0.9.0.0 introduced several features that increases security in a cluster. For detailed
description about these possibilities, see [Kafka security docs](http://kafka.apache.org/documentation.html#security).

It's worth noting that security is optional and turned off by default.

Spark supports the following ways to authenticate against Kafka cluster:
- **Delegation token (introduced in Kafka broker 1.1.0)**
- **JAAS login configuration**

### Delegation token

This way the application can be configured via Spark parameters and may not need JAAS login
configuration (Spark can use Kafka's dynamic JAAS configuration feature). For further information
about delegation tokens, see [Kafka delegation token docs](http://kafka.apache.org/documentation/#security_delegation_token).

The process is initiated by Spark's Kafka delegation token provider. When `spark.kafka.clusters.${cluster}.auth.bootstrap.servers` is set,
Spark considers the following log in options, in order of preference:
- **JAAS login configuration**, please see example below.
- **Keytab file**, such as,

      ./bin/spark-submit \
          --keytab <KEYTAB_FILE> \
          --principal <PRINCIPAL> \
          --conf spark.kafka.clusters.${cluster}.auth.bootstrap.servers=<KAFKA_SERVERS> \
          ...

- **Kerberos credential cache**, such as,

      ./bin/spark-submit \
          --conf spark.kafka.clusters.${cluster}.auth.bootstrap.servers=<KAFKA_SERVERS> \
          ...

The Kafka delegation token provider can be turned off by setting `spark.security.credentials.kafka.enabled` to `false` (default: `true`).

Spark can be configured to use the following authentication protocols to obtain token (it must match with
Kafka broker configuration):
- **SASL SSL (default)**
- **SSL**
- **SASL PLAINTEXT (for testing)**

After obtaining delegation token successfully, Spark distributes it across nodes and renews it accordingly.
Delegation token uses `SCRAM` login module for authentication and because of that the appropriate
`spark.kafka.clusters.${cluster}.sasl.token.mechanism` (default: `SCRAM-SHA-512`) has to be configured. Also, this parameter
must match with Kafka broker configuration.

When delegation token is available on an executor Spark considers the following log in options, in order of preference:
- **JAAS login configuration**, please see example below.
- **Delegation token**, please see <code>spark.kafka.clusters.${cluster}.target.bootstrap.servers.regex</code> parameter for further details.

When none of the above applies then unsecure connection assumed.


#### Configuration

Delegation tokens can be obtained from multiple clusters and <code>${cluster}</code> is an arbitrary unique identifier which helps to group different configurations.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td><code>spark.kafka.clusters.${cluster}.auth.bootstrap.servers</code></td>
    <td>None</td>
    <td>
      A list of coma separated host/port pairs to use for establishing the initial connection
      to the Kafka cluster. For further details please see Kafka documentation. Only used to obtain delegation token.
    </td>
  </tr>
  <tr>
    <td><code>spark.kafka.clusters.${cluster}.target.bootstrap.servers.regex</code></td>
    <td>.*</td>
    <td>
      Regular expression to match against the <code>bootstrap.servers</code> config for sources and sinks in the application.
      If a server address matches this regex, the delegation token obtained from the respective bootstrap servers will be used when connecting.
      If multiple clusters match the address, an exception will be thrown and the query won't be started.
      Kafka's secure and unsecure listeners are bound to different ports. When both used the secure listener port has to be part of the regular expression.
    </td>
  </tr>
  <tr>
    <td><code>spark.kafka.clusters.${cluster}.security.protocol</code></td>
    <td>SASL_SSL</td>
    <td>
      Protocol used to communicate with brokers. For further details please see Kafka documentation. Only used to obtain delegation token.
    </td>
  </tr>
  <tr>
    <td><code>spark.kafka.clusters.${cluster}.sasl.kerberos.service.name</code></td>
    <td>kafka</td>
    <td>
      The Kerberos principal name that Kafka runs as. This can be defined either in Kafka's JAAS config or in Kafka's config.
      For further details please see Kafka documentation. Only used to obtain delegation token.
    </td>
  </tr>
  <tr>
    <td><code>spark.kafka.clusters.${cluster}.ssl.truststore.location</code></td>
    <td>None</td>
    <td>
      The location of the trust store file. For further details please see Kafka documentation. Only used to obtain delegation token.
    </td>
  </tr>
  <tr>
    <td><code>spark.kafka.clusters.${cluster}.ssl.truststore.password</code></td>
    <td>None</td>
    <td>
      The store password for the trust store file. This is optional and only needed if <code>spark.kafka.clusters.${cluster}.ssl.truststore.location</code> is configured.
      For further details please see Kafka documentation. Only used to obtain delegation token.
    </td>
  </tr>
  <tr>
    <td><code>spark.kafka.clusters.${cluster}.ssl.keystore.location</code></td>
    <td>None</td>
    <td>
      The location of the key store file. This is optional for client and can be used for two-way authentication for client.
      For further details please see Kafka documentation. Only used to obtain delegation token.
    </td>
  </tr>
  <tr>
    <td><code>spark.kafka.clusters.${cluster}.ssl.keystore.password</code></td>
    <td>None</td>
    <td>
      The store password for the key store file. This is optional and only needed if <code>spark.kafka.clusters.${cluster}.ssl.keystore.location</code> is configured.
      For further details please see Kafka documentation. Only used to obtain delegation token.
    </td>
  </tr>
  <tr>
    <td><code>spark.kafka.clusters.${cluster}.ssl.key.password</code></td>
    <td>None</td>
    <td>
      The password of the private key in the key store file. This is optional for client.
      For further details please see Kafka documentation. Only used to obtain delegation token.
    </td>
  </tr>
  <tr>
    <td><code>spark.kafka.clusters.${cluster}.sasl.token.mechanism</code></td>
    <td>SCRAM-SHA-512</td>
    <td>
      SASL mechanism used for client connections with delegation token. Because SCRAM login module used for authentication a compatible mechanism has to be set here.
      For further details please see Kafka documentation (<code>sasl.mechanism</code>). Only used to authenticate against Kafka broker with delegation token.
    </td>
  </tr>
</table>

#### Caveats

- Obtaining delegation token for proxy user is not yet supported ([KAFKA-6945](https://issues.apache.org/jira/browse/KAFKA-6945)).

### JAAS login configuration

JAAS login configuration must placed on all nodes where Spark tries to access Kafka cluster.
This provides the possibility to apply any custom authentication logic with a higher cost to maintain.
This can be done several ways. One possibility is to provide additional JVM parameters, such as,

    ./bin/spark-submit \
        --driver-java-options "-Djava.security.auth.login.config=/path/to/custom_jaas.conf" \
        --conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/path/to/custom_jaas.conf \
        ...
