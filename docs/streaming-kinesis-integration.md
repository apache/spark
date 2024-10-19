---
layout: global
title: Spark Streaming + Kinesis Integration
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
[Amazon Kinesis](http://aws.amazon.com/kinesis/) is a fully managed service for real-time processing of streaming data at massive scale.
The Kinesis receiver creates an input DStream using the Kinesis Client Library (KCL) provided by Amazon under the Amazon Software License (ASL).
The KCL builds on top of the Apache 2.0 licensed AWS Java SDK and provides load-balancing, fault-tolerance, checkpointing through the concepts of Workers, Checkpoints, and Shard Leases.
Here we explain how to configure Spark Streaming to receive data from Kinesis.

#### Configuring Kinesis

A Kinesis stream can be set up at one of the valid Kinesis endpoints with 1 or more shards per the following
[guide](http://docs.aws.amazon.com/kinesis/latest/dev/step-one-create-stream.html).


#### Configuring Spark Streaming Application

1. **Linking:** For Scala/Java applications using SBT/Maven project definitions, link your streaming application against the following artifact (see [Linking section](streaming-programming-guide.html#linking) in the main programming guide for further information).

        groupId = org.apache.spark
        artifactId = spark-streaming-kinesis-asl_{{site.SCALA_BINARY_VERSION}}
        version = {{site.SPARK_VERSION_SHORT}}

    For Python applications, you will have to add this above library and its dependencies when deploying your application. See the *Deploying* subsection below.
    **Note that by linking to this library, you will include [ASL](https://aws.amazon.com/asl/)-licensed code in your application.**

2. **Programming:** In the streaming application code, import `KinesisInputDStream` and create the input DStream of byte array as follows:

    <div class="codetabs">

    <div data-lang="python" markdown="1">
    ```python
    from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

    kinesisStream = KinesisUtils.createStream(
        streamingContext, [Kinesis app name], [Kinesis stream name], [endpoint URL],
        [region name], [initial position], [checkpoint interval], [metricsLevel.DETAILED],
        StorageLevel.MEMORY_AND_DISK_2)
    ```

    See the [API docs](api/python/reference/pyspark.streaming.html#kinesis)
    and the [example]({{site.SPARK_GITHUB_URL}}/tree/master/connector/kinesis-asl/src/main/python/examples/streaming/kinesis_wordcount_asl.py). Refer to the [Running the Example](#running-the-example) subsection for instructions to run the example.

    - CloudWatch metrics level and dimensions. See [the AWS documentation about monitoring KCL](https://docs.aws.amazon.com/streams/latest/dev/monitoring-with-kcl.html) for details. Default is `MetricsLevel.DETAILED`.

    </div>

    <div data-lang="scala" markdown="1">
    ```scala
    import org.apache.spark.storage.StorageLevel
    import org.apache.spark.streaming.kinesis.KinesisInputDStream
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    import org.apache.spark.streaming.kinesis.KinesisInitialPositions

    val kinesisStream = KinesisInputDStream.builder
        .streamingContext(streamingContext)
        .endpointUrl([endpoint URL])
        .regionName([region name])
        .streamName([streamName])
        .initialPosition([initial position])
        .checkpointAppName([Kinesis app name])
        .checkpointInterval([checkpoint interval])
        .metricsLevel([metricsLevel.DETAILED])
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    ```

    See the [API docs](api/scala/org/apache/spark/streaming/kinesis/KinesisInputDStream$.html)
    and the [example]({{site.SPARK_GITHUB_URL}}/tree/master/connector/kinesis-asl/src/main/scala/org/apache/spark/examples/streaming/KinesisWordCountASL.scala). Refer to the [Running the Example](#running-the-example) subsection for instructions on how to run the example.

    </div>

    <div data-lang="java" markdown="1">
    ```java
    import org.apache.spark.storage.StorageLevel;
    import org.apache.spark.streaming.kinesis.KinesisInputDStream;
    import org.apache.spark.streaming.Seconds;
    import org.apache.spark.streaming.StreamingContext;
    import org.apache.spark.streaming.kinesis.KinesisInitialPositions;

    KinesisInputDStream<byte[]> kinesisStream = KinesisInputDStream.builder()
        .streamingContext(streamingContext)
        .endpointUrl([endpoint URL])
        .regionName([region name])
        .streamName([streamName])
        .initialPosition([initial position])
        .checkpointAppName([Kinesis app name])
        .checkpointInterval([checkpoint interval])
        .metricsLevel([metricsLevel.DETAILED])
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build();
    ```

    See the [API docs](api/java/org/apache/spark/streaming/kinesis/package-summary.html)
    and the [example]({{site.SPARK_GITHUB_URL}}/tree/master/connector/kinesis-asl/src/main/java/org/apache/spark/examples/streaming/JavaKinesisWordCountASL.java). Refer to the [Running the Example](#running-the-example) subsection for instructions to run the example.

    </div>

    </div>

    You may also provide the following settings. This is currently only supported in Scala and Java.

    - A "message handler function" that takes a Kinesis `Record` and returns a generic object `T`, in case you would like to use other data included in a `Record` such as partition key.

    <div class="codetabs">
    <div data-lang="scala" markdown="1">
    ```scala
    import collection.JavaConverters._
    import org.apache.spark.storage.StorageLevel
    import org.apache.spark.streaming.kinesis.KinesisInputDStream
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    import org.apache.spark.streaming.kinesis.KinesisInitialPositions
    import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
    import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel

    val kinesisStream = KinesisInputDStream.builder
        .streamingContext(streamingContext)
        .endpointUrl([endpoint URL])
        .regionName([region name])
        .streamName([streamName])
        .initialPosition([initial position])
        .checkpointAppName([Kinesis app name])
        .checkpointInterval([checkpoint interval])
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .metricsLevel(MetricsLevel.DETAILED)
        .metricsEnabledDimensions(KinesisClientLibConfiguration.DEFAULT_METRICS_ENABLED_DIMENSIONS.asScala.toSet)
        .buildWithMessageHandler([message handler])
    ```

    </div>
    <div data-lang="java" markdown="1">
    ```java
    import org.apache.spark.storage.StorageLevel;
    import org.apache.spark.streaming.kinesis.KinesisInputDStream;
    import org.apache.spark.streaming.Seconds;
    import org.apache.spark.streaming.StreamingContext;
    import org.apache.spark.streaming.kinesis.KinesisInitialPositions;
    import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
    import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
    import scala.collection.JavaConverters;

    KinesisInputDStream<byte[]> kinesisStream = KinesisInputDStream.builder()
        .streamingContext(streamingContext)
        .endpointUrl([endpoint URL])
        .regionName([region name])
        .streamName([streamName])
        .initialPosition([initial position])
        .checkpointAppName([Kinesis app name])
        .checkpointInterval([checkpoint interval])
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .metricsLevel(MetricsLevel.DETAILED)
        .metricsEnabledDimensions(
            JavaConverters.asScalaSetConverter(
                KinesisClientLibConfiguration.DEFAULT_METRICS_ENABLED_DIMENSIONS
            )
            .asScala().toSet()
        )
        .buildWithMessageHandler([message handler]);
    ```

    </div>
    </div>

    - `streamingContext`: StreamingContext containing an application name used by Kinesis to tie this Kinesis application to the Kinesis stream

    - `[Kinesis app name]`: The application name that will be used to checkpoint the Kinesis
        sequence numbers in DynamoDB table.
        - The application name must be unique for a given account and region.
        - If the table exists but has incorrect checkpoint information (for a different stream, or
            old expired sequenced numbers), then there may be temporary errors.

    - `[Kinesis stream name]`: The Kinesis stream that this streaming application will pull data from.

    - `[endpoint URL]`: Valid Kinesis endpoints URL can be found [here](http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region).

    - `[region name]`: Valid Kinesis region names can be found [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html).

    - `[checkpoint interval]`: The interval (e.g., Duration(2000) = 2 seconds) at which the Kinesis Client Library saves its position in the stream.  For starters, set it to the same as the batch interval of the streaming application.

    - `[initial position]`: Can be either `KinesisInitialPositions.TrimHorizon` or `KinesisInitialPositions.Latest` or `KinesisInitialPositions.AtTimestamp` (see [`Kinesis Checkpointing`](#kinesis-checkpointing) section and [`Amazon Kinesis API documentation`](http://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-sdk.html) for more details).

    - `[message handler]`: A function that takes a Kinesis `Record` and outputs generic `T`.

    In other versions of the API, you can also specify the AWS access key and secret key directly.

3. **Deploying:** As with any Spark applications, `spark-submit` is used to launch your application. However, the details are slightly different for Scala/Java applications and Python applications.

    For Scala and Java applications, if you are using SBT or Maven for project management, then package `spark-streaming-kinesis-asl_{{site.SCALA_BINARY_VERSION}}` and its dependencies into the application JAR. Make sure `spark-core_{{site.SCALA_BINARY_VERSION}}` and `spark-streaming_{{site.SCALA_BINARY_VERSION}}` are marked as `provided` dependencies as those are already present in a Spark installation. Then use `spark-submit` to launch your application (see [Deploying section](streaming-programming-guide.html#deploying-applications) in the main programming guide).

    For Python applications which lack SBT/Maven project management, `spark-streaming-kinesis-asl_{{site.SCALA_BINARY_VERSION}}` and its dependencies can be directly added to `spark-submit` using `--packages` (see [Application Submission Guide](submitting-applications.html)). That is,

        ./bin/spark-submit --packages org.apache.spark:spark-streaming-kinesis-asl_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} ...

    Alternatively, you can also download the JAR of the Maven artifact `spark-streaming-kinesis-asl-assembly` from the
    [Maven repository](http://search.maven.org/#search|ga|1|a%3A%22spark-streaming-kinesis-asl-assembly_{{site.SCALA_BINARY_VERSION}}%22%20AND%20v%3A%22{{site.SPARK_VERSION_SHORT}}%22) and add it to `spark-submit` with `--jars`.

    <p style="text-align: center;">
          <img src="img/streaming-kinesis-arch.png"
               title="Spark Streaming Kinesis Architecture"
               alt="Spark Streaming Kinesis Architecture"
           width="60%"
        />
    </p>

    *Points to remember at runtime:*

    - Kinesis data processing is ordered per partition and occurs at-least once per message.

    - Multiple applications can read from the same Kinesis stream.  Kinesis will maintain the application-specific shard and checkpoint info in DynamoDB.

    - A single Kinesis stream shard is processed by one input DStream at a time.

    - A single Kinesis input DStream can read from multiple shards of a Kinesis stream by creating multiple KinesisRecordProcessor threads.

    - Multiple input DStreams running in separate processes/instances can read from a Kinesis stream.

    - You never need more Kinesis input DStreams than the number of Kinesis stream shards as each input DStream will create at least one KinesisRecordProcessor thread that handles a single shard.

    - Horizontal scaling is achieved by adding/removing  Kinesis input DStreams (within a single process or across multiple processes/instances) - up to the total number of Kinesis stream shards per the previous point.

    - The Kinesis input DStream will balance the load between all DStreams - even across processes/instances.

    - The Kinesis input DStream will balance the load during re-shard events (merging and splitting) due to changes in load.

    - As a best practice, it's recommended that you avoid re-shard jitter by over-provisioning when possible.

    - Each Kinesis input DStream maintains its own checkpoint info.  See the Kinesis Checkpointing section for more details.

    - There is no correlation between the number of Kinesis stream shards and the number of RDD partitions/shards created across the Spark cluster during input DStream processing.  These are 2 independent partitioning schemes.

#### Running the Example
To run the example,

- Download a Spark binary from the [download site](https://spark.apache.org/downloads.html).

- Set up Kinesis stream (see earlier section) within AWS. Note the name of the Kinesis stream and the endpoint URL corresponding to the region where the stream was created.

- Set up the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` with your AWS credentials.

- In the Spark root directory, run the example as

    <div class="codetabs">

    <div data-lang="python" markdown="1">
    ```sh
    ./bin/spark-submit --jars 'connector/kinesis-asl-assembly/target/spark-streaming-kinesis-asl-assembly_*.jar' \
        connector/kinesis-asl/src/main/python/examples/streaming/kinesis_wordcount_asl.py \
        [Kinesis app name] [Kinesis stream name] [endpoint URL] [region name]
    ```
    </div>

    <div data-lang="scala" markdown="1">
    ```sh
    ./bin/run-example --packages org.apache.spark:spark-streaming-kinesis-asl_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} streaming.KinesisWordCountASL [Kinesis app name] [Kinesis stream name] [endpoint URL]
    ```
    </div>

    <div data-lang="java" markdown="1">
    ```sh
    ./bin/run-example --packages org.apache.spark:spark-streaming-kinesis-asl_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} streaming.JavaKinesisWordCountASL [Kinesis app name] [Kinesis stream name] [endpoint URL]
    ```
    </div>

    </div>

    This will wait for data to be received from the Kinesis stream.

- To generate random string data to put onto the Kinesis stream, in another terminal, run the associated Kinesis data producer.

    ```sh
    ./bin/run-example streaming.KinesisWordProducerASL [Kinesis stream name] [endpoint URL] 1000 10
    ```

    This will push 1000 lines per second of 10 random numbers per line to the Kinesis stream.  This data should then be received and processed by the running example.

#### Record De-aggregation

When data is generated using the [Kinesis Producer Library (KPL)](http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-kpl.html), messages may be aggregated for cost savings. Spark Streaming will automatically
de-aggregate records during consumption.

#### Kinesis Checkpointing
- Each Kinesis input DStream periodically stores the current position of the stream in the backing DynamoDB table.  This allows the system to recover from failures and continue processing where the DStream left off.

- Checkpointing too frequently will cause excess load on the AWS checkpoint storage layer and may lead to AWS throttling.  The provided example handles this throttling with a random-backoff-retry strategy.

- If no Kinesis checkpoint info exists when the input DStream starts, it will start either from the oldest record available (`KinesisInitialPositions.TrimHorizon`), or from the latest tip (`KinesisInitialPositions.Latest`), or (except Python) from the position denoted by the provided UTC timestamp (`KinesisInitialPositions.AtTimestamp(Date timestamp)`).  This is configurable.
  - `KinesisInitialPositions.Latest` could lead to missed records if data is added to the stream while no input DStreams are running (and no checkpoint info is being stored).
  - `KinesisInitialPositions.TrimHorizon` may lead to duplicate processing of records where the impact is dependent on checkpoint frequency and processing idempotency.

#### Kinesis retry configuration
 - `spark.streaming.kinesis.retry.waitTime` : Wait time between Kinesis retries as a duration string. When reading from Amazon Kinesis, users may hit `ProvisionedThroughputExceededException`'s, when consuming faster than 5 transactions/second or, exceeding the maximum read rate of 2 MiB/second. This configuration can be tweaked to increase the sleep between fetches when a fetch fails to reduce these exceptions. Default is "100ms".
 - `spark.streaming.kinesis.retry.maxAttempts` : Max number of retries for Kinesis fetches. This config can also be used to tackle the Kinesis `ProvisionedThroughputExceededException`'s in scenarios mentioned above. It can be increased to have more number of retries for Kinesis reads. Default is 3.
