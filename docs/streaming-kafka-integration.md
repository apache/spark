---
layout: global
title: Spark Streaming + Kafka Integration Guide
---
[Apache Kafka](http://kafka.apache.org/) is publish-subscribe messaging rethought as a distributed, partitioned, replicated commit log service.  Here we explain how to configure Spark Streaming to receive data from Kafka.

1. **Linking:** In your SBT/Maven projrect definition, link your streaming application against the following artifact (see [Linking section](streaming-programming-guide.html#linking) in the main programming guide for further information).

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
	and the [example]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/scala/org/apache/spark/examples/streaming/KafkaWordCount.scala).
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
