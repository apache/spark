---
layout: global
title: Structured Streaming + Kafka Integration Guide (Kafka broker version 0.10.0 or higher)
---

Structured Streaming integration for Kafka 0.10 to poll data from Kafka. It provides simple parallelism,
1:1 correspondence between Kafka partitions and Spark partitions.

### Linking
For Scala/Java applications using SBT/Maven project definitions, link your application with the following artifact:

    groupId = org.apache.spark
    artifactId = spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}
    version = {{site.SPARK_VERSION_SHORT}}

For Python applications, you need to add this above library and its dependencies when deploying your
application. See the [Deploying](#deploying) subsection below.

### Creating a Kafka Source Stream

<div class="codetabs">
<div data-lang="scala" markdown="1">

    // Subscribe to 1 topic
    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()
    ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    // Subscribe to multiple topics
    val ds2 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1,topic2")
      .load()
    ds2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    // Subscribe to a pattern
    val ds3 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribePattern", "topic.*")
      .load()
    ds3.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

</div>
<div data-lang="java" markdown="1">

    // Subscribe to 1 topic
    Dataset<Row> ds1 = spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()
    ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    // Subscribe to multiple topics
    Dataset<Row> ds2 = spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1,topic2")
      .load()
    ds2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    // Subscribe to a pattern
    Dataset<Row> ds3 = spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribePattern", "topic.*")
      .load()
    ds3.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

</div>
<div data-lang="python" markdown="1">

    # Subscribe to 1 topic
    ds1 = spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()
    ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Subscribe to multiple topics
    ds2 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1,topic2")
      .load()
    ds2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Subscribe to a pattern
    ds3 = spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribePattern", "topic.*")
      .load()
    ds3.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

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
  <td>long</td>
</tr>
<tr>
  <td>timestampType</td>
  <td>int</td>
</tr>
</table>

Right now, the Kafka source has the following Spark's specific options.

<table class="table">
<tr><th>Option</th><th>value</th><th>default</th><th>meaning</th></tr>
<tr>
  <td>startingOffset</td>
  <td>["earliest", "latest"]</td>
  <td>"latest"</td>
  <td>The start point when a query is started, either "earliest" which is from the earliest offset, 
  or "latest" which is just from the latest offset. Note: This only applies when a new Streaming q
  uery is started, and that resuming will always pick up from where the query left off.</td>
</tr>
<tr>
  <td>failOnCorruptMetadata</td>
  <td>[true, false]</td>
  <td>true</td>
  <td>Whether to fail the query when metadata is corrupt (e.g., topics are deleted, or offsets are 
  out of range), which may lost data.</td>
</tr>
<tr>
  <td>subscribe</td>
  <td>A comma-separated list of topics</td>
  <td>(none)</td>
  <td>The topic list to subscribe. Only one of "subscribe" and "subscribePattern" options can be 
  specified for Kafka source.</td>
</tr>
<tr>
  <td>subscribePattern</td>
  <td>Java regex string</td>
  <td>(none)</td>
  <td>The pattern used to subscribe the topic. Only one of "subscribe" and "subscribePattern" 
  options can be specified for Kafka source.</td>
</tr>
<tr>
  <td>kafka.consumer.poll.timeoutMs</td>
  <td>long</td>
  <td>512</td>
  <td>The timeout in milliseconds to poll data from Kafka in executors.</td>
</tr>
<tr>
  <td>fetchOffset.numRetries</td>
  <td>int</td>
  <td>3</td>
  <td>Number of times to retry before giving up fatch Kafka latest offsets.</td>
</tr>
<tr>
  <td>fetchOffset.retry.intervalMs</td>
  <td>long</td>
  <td>10</td>
  <td>milliseconds to wait before retrying to fetch Kafka offsets</td>
</tr>
</table>

Kafka's own configurations can be set via `DataStreamReader.option` with `kafka.` prefix, e.g, 
`stream.option("kafka.bootstrap.servers", "host:port")`. For possible kafkaParams, see 
[Kafka consumer config docs](http://kafka.apache.org/documentation.html#newconsumerconfigs).

Note that the following Kafka params cannot be set and the Kafka source will throw an exception:
- **group.id**: Kafka source will create a unique group id for each query automatically.
- **auto.offset.reset**: Set the source option `startingOffset` to `earliest` or `latest` to specify
 where to start instead. Structured Streaming manages which offsets are consumed internally, rather 
 than rely on the kafka Consumer to do it. This will ensure that no data is missed when when new 
 topics/partitions are dynamically subscribed. Note that `startingOffset` only applies when a new
 Streaming query is started, and that resuming will always pick up from where the query left off.
- **key.deserializer**: Keys are always deserialized as byte arrays with ByteArrayDeserializer. Use 
 Dataframe operations to explicitly deserialize the keys.
- **value.deserializer**: Values are always deserialized as byte arrays with ByteArrayDeserializer. 
 Use Dataframe operations to explicitly deserialize the values.
- **enable.auto.commit**: Kafka source doesn't commit any offset.
- **interceptor.classes**: Kafka source always read keys and values as byte arrays. It's not safe to
 use ConsumerInterceptor as it may break the query.

### Deploying

As with any Spark applications, `spark-submit` is used to launch your application. `spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}`
and its dependencies can be directly added to `spark-submit` using `--packages`, such as,

    ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} ...

See [Application Submission Guide](submitting-applications.html) for more details about submitting
applications with external dependencies.
