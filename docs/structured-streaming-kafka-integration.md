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
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()

    // Subscribe to multiple topics
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1,topic2")
      .load()

    // Subscribe to a pattern
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribePattern", "topic.*")
      .load()

</div>
<div data-lang="java" markdown="1">

    // Subscribe to 1 topic
    spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()

    // Subscribe to multiple topics
    spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1,topic2")
      .load()

    // Subscribe to a pattern
    spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribePattern", "topic.*")
      .load()

</div>
<div data-lang="python" markdown="1">

    # Subscribe to 1 topic
    spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()

    # Subscribe to multiple topics
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1,topic2")
      .load()

    # Subscribe to a pattern
    spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribePattern", "topic.*")
      .load()

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
  <td>Whether to fail the query when metadata is corrupt (e.g., topics are deleted), which can cause
   data lost.</td>
</tr>
<tr>
  <td>subscribe</td>
  <td>A comma-separated list of topics</td>
  <td>(none)</td>
  <td>The topic list to subscribe. Only one of "subscribe" and "subscribeParttern" options can be 
  specified for Kafka source.</td>
</tr>
<tr>
  <td>subscribeParttern</td>
  <td>Java regex string</td>
  <td>(none)</td>
  <td>The pattern used to subscribe the topic. Only one of "subscribe" and "subscribeParttern" 
  options can be specified for Kafka source.</td>
</tr>
</table>

Kafka's own configurations can be set via `DataStreamReader.option` with `kafka.` prefix, e.g, 
`stream.option("kafka.bootstrap.servers", "host:port")`. For possible kafkaParams, see 
[Kafka consumer config docs](http://kafka.apache.org/documentation.html#newconsumerconfigs).

### Deploying

As with any Spark applications, `spark-submit` is used to launch your application. `spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}`
and its dependencies can be directly added to `spark-submit` using `--packages`, such as,

    ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} ...

See [Application Submission Guide](submitting-applications.html) for more details about submitting
applications with external dependencies.
