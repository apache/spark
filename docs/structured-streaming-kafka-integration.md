---
layout: global
title: Structured Streaming + Kafka Integration Guide (Kafka broker version 0.10.0 or higher)
---

Structured Streaming integration for Kafka 0.10 to poll data from Kafka.

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
{% highlight scala %}

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

{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}

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

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}

# Subscribe to 1 topic
ds1 = spark
  .readStream
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
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribePattern", "topic.*")
  .load()
ds3.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

{% endhighlight %}
</div>
</div>

### Creating a Kafka Source Batch
If you have a use case that is better suited to batch processing,
you can create an Dataset/DataFrame for a defined range of offsets.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}

// Subscribe to 1 topic defaults to the earliest and latest offsets
val ds1 = spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .load()
ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to multiple topics, specifying explicit Kafka offsets
val ds2 = spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1,topic2")
  .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
  .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
  .load()
ds2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to a pattern, at the earliest and latest offsets
val ds3 = spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribePattern", "topic.*")
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
  .load()
ds3.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}

// Subscribe to 1 topic defaults to the earliest and latest offsets
Dataset<Row> ds1 = spark
  .read()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .load();
ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

// Subscribe to multiple topics, specifying explicit Kafka offsets
Dataset<Row> ds2 = spark
  .read()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1,topic2")
  .option("startingOffsets", "{\"topic1\":{\"0\":23,\"1\":-2},\"topic2\":{\"0\":-2}}")
  .option("endingOffsets", "{\"topic1\":{\"0\":50,\"1\":-1},\"topic2\":{\"0\":-1}}")
  .load();
ds2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

// Subscribe to a pattern, at the earliest and latest offsets
Dataset<Row> ds3 = spark
  .read()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribePattern", "topic.*")
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
  .load();
ds3.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}

# Subscribe to 1 topic defaults to the earliest and latest offsets
ds1 = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1") \
  .load()
ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Subscribe to multiple topics, specifying explicit Kafka offsets
ds2 = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1,topic2") \
  .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""") \
  .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""") \
  .load()
ds2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Subscribe to a pattern, at the earliest and latest offsets
ds3 = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribePattern", "topic.*") \
  .option("startingOffsets", "earliest") \
  .option("endingOffsets", "latest") \
  .load()
ds3.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

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
  <td>long</td>
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
  <td>streaming query</td>
  <td>Whether to fail the query when it's possible that data is lost (e.g., topics are deleted, or
  offsets are out of range). This may be a false alarm. You can disable it when it doesn't work
  as you expected. Batch queries will always fail if it fails to read any data from the provided
  offsets due to lost data.</td>
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
</table>

Kafka's own configurations can be set via `DataStreamReader.option` with `kafka.` prefix, e.g, 
`stream.option("kafka.bootstrap.servers", "host:port")`. For possible kafkaParams, see 
[Kafka consumer config docs](http://kafka.apache.org/documentation.html#newconsumerconfigs).

Note that the following Kafka params cannot be set and the Kafka source will throw an exception:

- **group.id**: Kafka source will create a unique group id for each query automatically.
- **auto.offset.reset**: Set the source option `startingOffsets` to specify
 where to start instead. Structured Streaming manages which offsets are consumed internally, rather 
 than rely on the kafka Consumer to do it. This will ensure that no data is missed when when new 
 topics/partitions are dynamically subscribed. Note that `startingOffsets` only applies when a new
 streaming query is started, and that resuming will always pick up from where the query left off.
- **key.deserializer**: Keys are always deserialized as byte arrays with ByteArrayDeserializer. Use 
 DataFrame operations to explicitly deserialize the keys.
- **value.deserializer**: Values are always deserialized as byte arrays with ByteArrayDeserializer. 
 Use DataFrame operations to explicitly deserialize the values.
- **enable.auto.commit**: Kafka source doesn't commit any offset.
- **interceptor.classes**: Kafka source always read keys and values as byte arrays. It's not safe to
 use ConsumerInterceptor as it may break the query.

### Deploying

As with any Spark applications, `spark-submit` is used to launch your application. `spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}`
and its dependencies can be directly added to `spark-submit` using `--packages`, such as,

    ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} ...

See [Application Submission Guide](submitting-applications.html) for more details about submitting
applications with external dependencies.
