---
layout: global
title: Spark Streaming + Kafka Integration Guide
---
[Apache Kafka](http://kafka.apache.org/) is publish-subscribe messaging rethought as a distributed, partitioned, replicated commit log service.  Here we explain how to configure Spark Streaming to receive data from Kafka. There are two approaches to this - the old approach using Receivers and Kafka's high-level API, and a new experimental approach (introduced in Spark 1.3) without using Receivers. They have different programming models, performance characteristics, and semantics guarantees, so read on for more details.  

## Approach 1: Receiver-based Approach
This approach uses a Receiver to receive the data. The Received is implemented using the Kafka high-level consumer API. As with all receivers, the data received from Kafka through a Receiver is stored in Spark executors, and then jobs launched by Spark Streaming processes the data. 

However, under default configuration, this approach can lose data under failures (see [receiver reliability](streaming-programming-guide.html#receiver-reliability). To ensure zero-data loss, you have to additionally enable Write Ahead Logs in Spark Streaming. To ensure zero data loss, enable the Write Ahead Logs (introduced in Spark 1.2). This synchronously saves all the received Kafka data into write ahead logs on a distributed file system (e.g HDFS), so that all the data can be recovered on failure. See [Deploying section](streaming-programming-guide.html#deploying-applications) in the streaming programming guide for more details on Write Ahead Logs.

Next, we discuss how to use this approach in your streaming application.

1. **Linking:** For Scala/Java applications using SBT/Maven project definitions, link your streaming application with the following artifact (see [Linking section](streaming-programming-guide.html#linking) in the main programming guide for further information).

		groupId = org.apache.spark
		artifactId = spark-streaming-kafka_{{site.SCALA_BINARY_VERSION}}
		version = {{site.SPARK_VERSION_SHORT}}

	For Python applications, you will have to add this above library and its dependencies when deploying your application. See the *Deploying* subsection below.

2. **Programming:** In the streaming application code, import `KafkaUtils` and create an input DStream as follows.

	<div class="codetabs">
	<div data-lang="scala" markdown="1">
		import org.apache.spark.streaming.kafka._

		val kafkaStream = KafkaUtils.createStream(streamingContext, 
            [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume])

    You can also specify the key and value classes and their corresponding decoder classes using variations of `createStream`. See the [API docs](api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$)
	and the [example]({{site.SPARK_GITHUB_URL}}/blob/master/examples/scala-2.10/src/main/scala/org/apache/spark/examples/streaming/KafkaWordCount.scala).
	</div>
	<div data-lang="java" markdown="1">
		import org.apache.spark.streaming.kafka.*;

		JavaPairReceiverInputDStream<String, String> kafkaStream = 
			KafkaUtils.createStream(streamingContext,
            [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume]);

    You can also specify the key and value classes and their corresponding decoder classes using variations of `createStream`. See the [API docs](api/java/index.html?org/apache/spark/streaming/kafka/KafkaUtils.html)
	and the [example]({{site.SPARK_GITHUB_URL}}/blob/master/examples/scala-2.10/src/main/java/org/apache/spark/examples/streaming/JavaKafkaWordCount.java).

	</div>
	<div data-lang="python" markdown="1">
		from pyspark.streaming.kafka import KafkaUtils

		kafkaStream = KafkaUtils.createStream(streamingContext, \
			[ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume])

	By default, the Python API will decode Kafka data as UTF8 encoded strings. You can specify your custom decoding function to decode the byte arrays in Kafka records to any arbitrary data type. See the [API docs](api/python/pyspark.streaming.html#pyspark.streaming.kafka.KafkaUtils)
	and the [example]({{site.SPARK_GITHUB_URL}}/blob/master/examples/src/main/python/streaming/kafka_wordcount.py).	
	</div>
	</div>

	**Points to remember:**

	- Topic partitions in Kafka does not correlate to partitions of RDDs generated in Spark Streaming. So increasing the number of topic-specific partitions in the `KafkaUtils.createStream()` only increases the number of threads using which topics that are consumed within a single receiver. It does not increase the parallelism of Spark in processing the data. Refer to the main document for more information on that.

	- Multiple Kafka input DStreams can be created with different groups and topics for parallel receiving of data using multiple receivers.

	- If you have enabled Write Ahead Logs with a replicated file system like HDFS, the received data is already being replicated in the log. Hence, the storage level in storage level for the input stream to `StorageLevel.MEMORY_AND_DISK_SER` (that is, use
`KafkaUtils.createStream(..., StorageLevel.MEMORY_AND_DISK_SER)`).

3. **Deploying:** As with any Spark applications, `spark-submit` is used to launch your application. However, the details are slightly different for Scala/Java applications and Python applications.

	For Scala and Java applications, if you are using SBT or Maven for project management, then package `spark-streaming-kafka_{{site.SCALA_BINARY_VERSION}}` and its dependencies into the application JAR. Make sure `spark-core_{{site.SCALA_BINARY_VERSION}}` and `spark-streaming_{{site.SCALA_BINARY_VERSION}}` are marked as `provided` dependencies as those are already present in a Spark installation. Then use `spark-submit` to launch your application (see [Deploying section](streaming-programming-guide.html#deploying-applications) in the main programming guide). 

	For Python applications which lack SBT/Maven project management, `spark-streaming-kafka_{{site.SCALA_BINARY_VERSION}}` and its dependencies can be directly added to `spark-submit` using `--packages` (see [Application Submission Guide](submitting-applications.html)). That is, 

	    ./bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} ...

	Alternatively, you can also download the JAR of the Maven artifact `spark-streaming-kafka-assembly` from the 
	[Maven repository](http://search.maven.org/#search|ga|1|a%3A%22spark-streaming-kafka-assembly_2.10%22%20AND%20v%3A%22{{site.SPARK_VERSION_SHORT}}%22) and add it to `spark-submit` with `--jars`.

## Approach 2: Direct Approach (No Receivers)
This is a new receiver-less "direct" approach has been introduced in Spark 1.3 to ensure stronger end-to-end guarantees. Instead of using receivers to receive data, this approach periodically queries Kafka for the latest offsets in each topic+partition, and accordingly defines the offset ranges to process in each batch. When the jobs to process the data are launched, Kafka's simple consumer API is used to read the defined ranges of offsets from Kafka (similar to read files from a file system). Note that this is an experimental feature in Spark 1.3 and is only available in the Scala and Java API.

This approach has the following advantages over the received-based approach (i.e. Approach 1).

- *Simplified Parallelism:* No need to create multiple input Kafka streams and union-ing them. With `directStream`, Spark Streaming will create as many RDD partitions as there is Kafka partitions to consume, which will all read data from Kafka in parallel. So there is one-to-one mapping between Kafka and RDD partitions, which is easier to understand and tune.

- *Efficiency:* Achieving zero-data loss in the first approach required the data to be stored in a Write Ahead Log, which further replicated the data. This is actually inefficient as the data effectively gets replicated twice - once by Kafka, and a second time by the Write Ahead Log. This second approach eliminate the problem as there is no receiver, and hence no need for Write Ahead Logs.

- *Exactly-once semantics:* The first approach uses Kafka's high level API to store consumed offsets in Zookeeper. This is traditionally the way to consume data from Kafka. While this approach (in combination with write ahead logs) can ensure zero data loss (i.e. at-least once semantics), there is a small chance some records may get consumed twice under some failures. This occurs because of inconsistencies between data reliably received by Spark Streaming and offsets tracked by Zookeeper. Hence, in this second approach, we use simple Kafka API that does not use Zookeeper and offsets tracked only by Spark Streaming within its checkpoints. This eliminates inconsistencies between Spark Streaming and Zookeeper/Kafka, and so each record is received by Spark Streaming effectively exactly once despite failures.

Note that one disadvantage of this approach is that it does not update offsets in Zookeeper, hence Zookeeper-based Kafka monitoring tools will not show progress. However, you can access the offsets processed by this approach in each batch and update Zookeeper yourself (see below).

Next, we discuss how to use this approach in your streaming application.

1. **Linking:** This approach is supported only in Scala/Java application. Link your SBT/Maven project with the following artifact (see [Linking section](streaming-programming-guide.html#linking) in the main programming guide for further information).

		groupId = org.apache.spark
		artifactId = spark-streaming-kafka_{{site.SCALA_BINARY_VERSION}}
		version = {{site.SPARK_VERSION_SHORT}}

2. **Programming:** In the streaming application code, import `KafkaUtils` and create an input DStream as follows.

	<div class="codetabs">
	<div data-lang="scala" markdown="1">
		import org.apache.spark.streaming.kafka._

		val directKafkaStream = KafkaUtils.createDirectStream[
			[key class], [value class], [key decoder class], [value decoder class] ](
			streamingContext, [map of Kafka parameters], [set of topics to consume])

	See the [API docs](api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$)
	and the [example]({{site.SPARK_GITHUB_URL}}/blob/master/examples/scala-2.10/src/main/scala/org/apache/spark/examples/streaming/DirectKafkaWordCount.scala).
	</div>
	<div data-lang="java" markdown="1">
		import org.apache.spark.streaming.kafka.*;

		JavaPairReceiverInputDStream<String, String> directKafkaStream = 
			KafkaUtils.createDirectStream(streamingContext,
				[key class], [value class], [key decoder class], [value decoder class],
				[map of Kafka parameters], [set of topics to consume]);

	See the [API docs](api/java/index.html?org/apache/spark/streaming/kafka/KafkaUtils.html)
	and the [example]({{site.SPARK_GITHUB_URL}}/blob/master/examples/scala-2.10/src/main/java/org/apache/spark/examples/streaming/JavaDirectKafkaWordCount.java).

	</div>
	</div>

	In the Kafka parameters, you must specify either `metadata.broker.list` or `bootstrap.servers`.
	By default, it will start consuming from the latest offset of each Kafka partition. If you set configuration `auto.offset.reset` in Kafka parameters to `smallest`, then it will start consuming from the smallest offset. 

	You can also start consuming from any arbitrary offset using other variations of `KafkaUtils.createDirectStream`. Furthermore, if you want to access the Kafka offsets consumed in each batch, you can do the following. 

	<div class="codetabs">
	<div data-lang="scala" markdown="1">
		directKafkaStream.foreachRDD { rdd => 
		    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges]
		    // offsetRanges.length = # of Kafka partitions being consumed
		    ...
		}
	</div>
	<div data-lang="java" markdown="1">
		directKafkaStream.foreachRDD(
		    new Function<JavaPairRDD<String, String>, Void>() {
		        @Override
		        public Void call(JavaPairRDD<String, Integer> rdd) throws IOException {
		            OffsetRange[] offsetRanges = ((HasOffsetRanges)rdd).offsetRanges
			        // offsetRanges.length = # of Kafka partitions being consumed
		            ...
		            return null;
		        }
		    }
		);
	</div>
	</div>

	You can use this to update Zookeeper yourself if you want Zookeeper-based Kafka monitoring tools to show progress of the streaming application.

	Another thing to note is that since this approach does not use Receivers, the standard receiver-related (that is, [configurations](configuration.html) of the form `spark.streaming.receiver.*` ) will not apply to the input DStreams created by this approach (will apply to other input DStreams though). Instead, use the [configurations](configuration.html) `spark.streaming.kafka.*`. An important one is `spark.streaming.kafka.maxRatePerPartition` which is the maximum rate at which each Kafka partition will be read by this direct API. 

3. **Deploying:** Similar to the first approach, you can package `spark-streaming-kafka_{{site.SCALA_BINARY_VERSION}}` and its dependencies into the application JAR and the launch the application using `spark-submit`. Make sure `spark-core_{{site.SCALA_BINARY_VERSION}}` and `spark-streaming_{{site.SCALA_BINARY_VERSION}}` are marked as `provided` dependencies as those are already present in a Spark installation.