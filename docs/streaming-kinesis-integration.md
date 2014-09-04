---
layout: global
title: Spark Streaming + Kinesis Integration
---
[Amazon Kinesis](http://aws.amazon.com/kinesis/) is a fully managed service for real-time processing of streaming data at massive scale.
The Kinesis input DStream and receiver uses the Kinesis Client Library (KCL) provided by Amazon under the Amazon Software License (ASL).
The KCL builds on top of the Apache 2.0 licensed AWS Java SDK and provides load-balancing, fault-tolerance, checkpointing through the concept of Workers, Checkpoints, and Shard Leases.
Here we explain how to configure Spark Streaming to receive data from Kinesis.

#### Configuring Kinesis

A Kinesis stream can be set up at one of the valid Kinesis endpoints with 1 or more shards per the following
[guide](http://docs.aws.amazon.com/kinesis/latest/dev/step-one-create-stream.html).


#### Configuring Spark Streaming Application

1. **Linking:** In your SBT/Maven projrect definition, link your streaming application against the following artifact (see [Linking section](streaming-programming-guide.html#linking) in the main programming guide for further information).

		groupId = org.apache.spark
		artifactId = spark-streaming-kinesis-asl_{{site.SCALA_BINARY_VERSION}}
		version = {{site.SPARK_VERSION_SHORT}}

	**Note that by linking to this library, you will include [ASL](https://aws.amazon.com/asl/)-licensed code in your application.**

2. **Programming:** In the streaming application code, import `KinesisUtils` and create input DStream as follows.

	<div class="codetabs">
	<div data-lang="scala" markdown="1">
		import org.apache.spark.streaming.kinesis._
		import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

		val kinesisStream = KinesisUtils.createStream(
        	streamingContext, [Kinesis stream name], [endpoint URL], [checkpoint interval], [initial position])

	See the [API docs](api/scala/index.html#org.apache.spark.streaming.kinesis.KinesisUtils$)
	and the [example]({{site.SPARK_GITHUB_URL}}/tree/master/extras/kinesis-asl/src/main/scala/org/apache/spark/examples/streaming/KinesisWordCountASL.scala). Refer to the next subsection for instructions to run the example.

	</div>
	<div data-lang="java" markdown="1">
		import org.apache.spark.streaming.flume.*;

		JavaReceiverInputDStream<byte[]> kinesisStream = KinesisUtils.createStream(
        	streamingContext, [Kinesis stream name], [endpoint URL], [checkpoint interval], [initial position]);

	See the [API docs](api/java/index.html?org/apache/spark/streaming/kinesis/KinesisUtils.html)
	and the [example]({{site.SPARK_GITHUB_URL}}/tree/master/extras/kinesis-asl/src/main/java/org/apache/spark/examples/streaming/JavaKinesisWordCountASL.java). Refer to the next subsection for instructions to run the example.

	</div>
	</div>

	`[endpoint URL]`: Valid Kinesis endpoints URL can be found [here](http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region).

	`[checkpoint interval]`: The interval at which the Kinesis client library is going to save its position in the stream. For starters, set it to the same as the batch interval of the streaming application.

	`[initial position]`: Can be either `InitialPositionInStream.TRIM_HORIZON` or `InitialPositionInStream.LATEST` (see later section and Amazon Kinesis API documentation for more details).

	*Points to remember:*

	- The name used in the context of the streaming application must be unique for a given account and region. Changing the app name or stream name could lead to Kinesis errors as only a single logical application can process a single stream.
	- A single Kinesis input DStream can receive many Kinesis shards by spinning up multiple KinesisRecordProcessor threads. Note that there is no correlation between number of shards in Kinesis and the number of partitions in the generated RDDs that is used for processing the data.
	- You never need more KinesisReceivers than the number of shards in your stream as each will spin up at least one KinesisRecordProcessor thread.
	- Horizontal scaling is achieved by autoscaling additional Kinesis input DStreams (separate processes) up to the number of current shards for a given stream, of course.

3. **Deploying:** Package `spark-streaming-flume_{{site.SCALA_BINARY_VERSION}}` and its dependencies (except `spark-core_{{site.SCALA_BINARY_VERSION}}` and `spark-streaming_{{site.SCALA_BINARY_VERSION}}` which are provided by `spark-submit`) into the application JAR. Then use `spark-submit` to launch your application (see [Deploying section](streaming-programming-guide.html#deploying-applications) in the main programming guide).

    - A DynamoDB table and CloudWatch namespace are created during KCL initialization using this Kinesis application name.  This DynamoDB table lives in the us-east-1 region regardless of the Kinesis endpoint URL. It is used to store KCL's checkpoint information.

    - If you are seeing errors after changing the app name or stream name, it may be necessary to manually delete the DynamoDB table and start from scratch.

#### Running the Example
To run the example,
- Download Spark source and follow the [instructions](building-with-maven.html) to build Spark with profile *-Pkinesis-asl*.

    mvn -Pkinesis-asl -DskipTests clean package

- Set up Kinesis stream (see earlier section). Note the name of the Kinesis stream, and the endpoint URL corresponding to the region the stream is based on.

- Set up the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_KEY with your AWS credentials.

- In the Spark root directory, run the example as
	<div class="codetabs">
	<div data-lang="scala" markdown="1">

    	bin/run-example streaming.KinesisWordCountASL [Kinesis stream name] [endpoint URL]

	</div>
	<div data-lang="java" markdown="1">

        bin/run-example streaming.JavaKinesisWordCountASL [Kinesis stream name] [endpoint URL]

	</div>
	</div>

    This will wait for data to be received from Kinesis.

- To generate random string data, in another terminal, run the associated Kinesis data producer.

		bin/run-example streaming.KinesisWordCountProducerASL [Kinesis stream name] [endpoint URL] 1000 10

	This will push random words to the Kinesis stream, which should then be received and processed by the running example.

#### Kinesis Checkpointing
The Kinesis receiver checkpoints the position of the stream that has been read periodically, so that the system can recover from failures and continue processing where it had left off. Checkpointing too frequently will cause excess load on the AWS checkpoint storage layer and may lead to AWS throttling.  The provided example handles this throttling with a random-backoff-retry strategy.

- If no Kinesis checkpoint info exists, the KinesisReceiver will start either from the oldest record available (InitialPositionInStream.TRIM_HORIZON) or from the latest tip (InitialPostitionInStream.LATEST).  This is configurable.

- InitialPositionInStream.LATEST could lead to missed records if data is added to the stream while no KinesisReceivers are running (and no checkpoint info is being stored). In production, you'll want to switch to InitialPositionInStream.TRIM_HORIZON which will read up to 24 hours (Kinesis limit) of previous stream data.

- InitialPositionInStream.TRIM_HORIZON may lead to duplicate processing of records where the impact is dependent on checkpoint frequency.
