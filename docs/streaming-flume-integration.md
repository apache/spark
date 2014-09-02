---
layout: global
title: Spark Streaming + Flume Integration Guide
---

[Apache Flume](https://flume.apache.org/) is a distributed, reliable, and available service for efficiently collecting, aggregating, and moving large amounts of log data. Here we explain how to configure the Flume and Spark Streaming. There are two approaches by which this integration can be achieved.

## Approach 1: Flume-style Push-based Approach
Flume is designed to push data between Flume agents. In this approach, Spark Streaming essentially sets up a receiver that acts an Avro agent for Flume, to which Flume can push the data. Here are the configuration steps.

#### General Requirements
Choose a machine in your cluster such that

- When your Flume + Spark Streaming application is launched, one of the Spark workers must run on that machine.

- Flume can be configured to push data to a port on that machine.

Due to the push model, the streaming application needs to be up, with the receiver scheduled and listening on the chosen port, for Flume to be able push data.

#### Configuring Flume
Configure Flume agent to send data to an Avro sink by having the following in the configuration file.

	agent.sinks = avroSink
	agent.sinks.avroSink.type = avro
    agent.sinks.avroSink.channel = memoryChannel
    agent.sinks.avroSink.hostname = <chosen machine's hostname>
	agent.sinks.avroSink.port = <chosen port on the machine>

See the [Flume's documentation](https://flume.apache.org/documentation.html) for more information about
configuring Flume agents.

#### Configuring Spark Streaming Application
1. **Linking:** In your SBT/Maven projrect definition, link your streaming application against the `spark-streaming-flume_{{site.SCALA_BINARY_VERSION}}` (see [Linking section](streaming-programming-guide.html#linking) in the main programming guide).

2. **Programming:** In the streaming application code, import `FlumeUtils` and create input DStream as follows.

	<div class="codetabs">
	<div data-lang="scala" markdown="1">
		import org.apache.spark.streaming.flume._

		val flumeStream = FlumeUtils.createStream(streamingContext, [chosen machine's hostname], [chosen port])

	See the example [FlumeEventCount]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/scala/org/apache/spark/examples/streaming/FlumeEventCount.scala).
	</div>
	<div data-lang="java" markdown="1">
		import org.apache.spark.streaming.flume.*;

		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
        	FlumeUtils.createStream(streamingContext, [chosen machine's hostname], [chosen port]);

	See the example [JavaFlumeEventCount]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/java/org/apache/spark/examples/streaming/JavaFlumeEventCount.java).
	</div>
	</div>

	Note that the hostname should be the same as the one used by the resource manager in the
    cluster (Mesos, YARN or Spark Standalone), so that resource allocation can match the names and launch
    the receiver in the right machine.

3. **Deploying:** Package `spark-streaming-flume_{{site.SCALA_BINARY_VERSION}}` and its dependencies (except `spark-core_{{site.SCALA_BINARY_VERSION}}` and `spark-streaming_{{site.SCALA_BINARY_VERSION}}` which are provided by `spark-submit`) into the application JAR. Then use `spark-submit` to launch your application (see [Deploying section](streaming-programming-guide.html#deploying-applications) in the main programming guide).

## Approach 2 (Experimental): Pull-based Approach using a Custom Sink
Instead of Flume pushing data directly to Spark Streaming, this approach runs a custom Flume sink that allows the following.
- Flume pushes data into the sink, and the data stays buffered.
- Spark Streaming uses transactions to pull data from the sink. Transactions succeed only after data is received and replicated by Spark Streaming.
This ensures that better reliability and fault-tolerance than the previous approach. However, this requires configuring Flume to run an extra agent with the custom sink. Here are the configuration steps.

#### General Requirements
Choose a machine that will run the custom sink in a Flume agent. The rest of the Flume pipeline is configured to Machines confiMachines in the Spark cluster should have access to the machine in the Flume cluster that is configured to run the custom sink.

#### Configuring Flume
Configuring Flume on the chosen machine requires the following two steps.

1. **Sink JARs**: Add the following JARs to Flume's classpath (see [Flume's documentation](https://flume.apache.org/documentation.html) to see how) in the machine designated to run the custom sink .

	(i) *Custom sink JAR*: Download the JAR corresponding to the following artifact (or [direct link](http://search.maven.org/remotecontent?filepath=org/apache/spark/spark-streaming-flume-sink_{{site.SCALA_BINARY_VERSION}}/{{site.SPARK_VERSION_SHORT}}/spark-streaming-flume-sink_{{site.SCALA_BINARY_VERSION}}-{{site.SPARK_VERSION_SHORT}}.jar)).

		groupId = org.apache.spark
		artifactId = spark-streaming-flume-sink_{{site.SCALA_BINARY_VERSION}}
		version = {{site.SPARK_VERSION_SHORT}}

	(ii) *Scala library JAR*: Download the Scala library JAR for Scala {{site.SCALA_VERSION}}. It can be found with the following artifact detail (or, [direct link](http://search.maven.org/remotecontent?filepath=org/scala-lang/scala-library/{{site.SCALA_VERSION}}/scala-library-{{site.SCALA_VERSION}}.jar)).

		groupId = org.scala-lang
		artifactId = scala-library
		version = {{site.SCALA_VERSION}}

2. **Configuration file**: On that machine, configure Flume agent to send data to an Avro sink by having the following in the configuration file.
		agent.sinks = spark
		agent.sinks.spark.type = org.apache.spark.streaming.flume.sink.SparkSink
		agent.sinks.spark.hostname = <hostname of the local machine>
		agent.sinks.spark.port = <port to listen on for connection from Spark>
		agent.sinks.spark.channel = memoryChannel

	Also make sure that the upstream Flume pipeline is configured to send the data to the Flume agent running this sink.

See the [Flume's documentation](https://flume.apache.org/documentation.html) for more information about
configuring Flume agents.

#### Configuring Spark Streaming Application
1. **Linking:** In your SBT/Maven projrect definition, link your streaming application against the `spark-streaming-flume_{{site.SCALA_BINARY_VERSION}}` (see [Linking section](streaming-programming-guide.html#linking) in the main programming guide).

2. **Programming:** In the streaming application code, import `FlumeUtils` and create input DStream as follows.

	<div class="codetabs">
	<div data-lang="scala" markdown="1">
		import org.apache.spark.streaming.flume._

		val flumeStream = FlumeUtils.createPollingStream(streamingContext, [sink machine hostname], [sink port])
	</div>
	<div data-lang="java" markdown="1">
		import org.apache.spark.streaming.flume.*;

		JavaReceiverInputDStream<SparkFlumeEvent>flumeStream =
			FlumeUtils.createPollingStream(streamingContext, [sink machine hostname], [sink port]);
	</div>
	</div>

	See the Scala example [FlumePollingEventCount]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/scala/org/apache/spark/examples/streaming/FlumePollingEventCount.scala).

3. **Deploying:** Package `spark-streaming-flume_{{site.SCALA_BINARY_VERSION}}` and its dependencies (except `spark-core_{{site.SCALA_BINARY_VERSION}}` and `spark-streaming_{{site.SCALA_BINARY_VERSION}}` which are provided by `spark-submit`) into the application JAR. Then use `spark-submit` to launch your application (see [Deploying section](streaming-programming-guide.html#deploying-applications) in the main programming guide).



