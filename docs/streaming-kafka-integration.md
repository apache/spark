---
layout: global
title: Spark Streaming + Kafka Integration Guide
---
[Apache Kafka](http://kafka.apache.org/) is publish-subscribe messaging rethought as a distributed, partitioned, replicated commit log service.  Here we explain how to configure Spark Streaming to receive data from Kafka.

1. **Linking:** In your SBT/Maven project definition, link your streaming application against the following artifact (see [Linking section](streaming-programming-guide.html#linking) in the main programming guide for further information).

		groupId = org.apache.spark
		artifactId = spark-streaming-kafka_{{site.SCALA_BINARY_VERSION}}
		version = {{site.SPARK_VERSION_SHORT}}

2. **Programming:** In the streaming application code, import `KafkaUtils` and create input DStream as follows.

	<div class="codetabs">
	<div data-lang="scala" markdown="1">
		import org.apache.spark.streaming.kafka._

		val kafkaStream = KafkaUtils.createStream(
        	streamingContext, [zookeeperQuorum], [group id of the consumer], [per-topic number of Kafka partitions to consume])

	See the [API docs](api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$)
	and the [example]({{site.SPARK_GITHUB_URL}}/blob/master/examples/scala-2.10/src/main/scala/org/apache/spark/examples/streaming/KafkaWordCount.scala).
	</div>
	<div data-lang="java" markdown="1">
		import org.apache.spark.streaming.kafka.*;

		JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(
        	streamingContext, [zookeeperQuorum], [group id of the consumer], [per-topic number of Kafka partitions to consume]);

	See the [API docs](api/java/index.html?org/apache/spark/streaming/kafka/KafkaUtils.html)
	and the [example]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/java/org/apache/spark/examples/streaming/JavaKafkaWordCount.java).
	</div>
	</div>

	*Points to remember:*

	- Topic partitions in Kafka does not correlate to partitions of RDDs generated in Spark Streaming. So increasing the number of topic-specific partitions in the `KafkaUtils.createStream()` only increases the number of threads using which topics that are consumed within a single receiver. It does not increase the parallelism of Spark in processing the data. Refer to the main document for more information on that.

	- Multiple Kafka input DStreams can be created with different groups and topics for parallel receiving of data using multiple receivers.

3. **Deploying:** Package `spark-streaming-kafka_{{site.SCALA_BINARY_VERSION}}` and its dependencies (except `spark-core_{{site.SCALA_BINARY_VERSION}}` and `spark-streaming_{{site.SCALA_BINARY_VERSION}}` which are provided by `spark-submit`) into the application JAR. Then use `spark-submit` to launch your application (see [Deploying section](streaming-programming-guide.html#deploying-applications) in the main programming guide).

Note that the Kafka receiver used by default is an
[*unreliable* receiver](streaming-programming-guide.html#receiver-reliability) section in the
programming guide). In Spark 1.2, we have added an experimental *reliable* Kafka receiver that
provides stronger
[fault-tolerance guarantees](streaming-programming-guide.html#fault-tolerance-semantics) of zero
data loss on failures. This receiver is automatically used when the write ahead log
(also introduced in Spark 1.2) is enabled
(see [Deployment](#deploying-applications.html) section in the programming guide). This
may reduce the receiving throughput of individual Kafka receivers compared to the unreliable
receivers, but this can be corrected by running
[more receivers in parallel](streaming-programming-guide.html#level-of-parallelism-in-data-receiving)
to increase aggregate throughput. Additionally, it is recommended that the replication of the
received data within Spark be disabled when the write ahead log is enabled as the log is already stored
in a replicated storage system. This can be done by setting the storage level for the input
stream to `StorageLevel.MEMORY_AND_DISK_SER` (that is, use
`KafkaUtils.createStream(..., StorageLevel.MEMORY_AND_DISK_SER)`).
