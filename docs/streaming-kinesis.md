---
layout: global
title: Spark Streaming Kinesis Receiver
---

## Kinesis
###Design
<li>The KinesisReceiver uses the Kinesis Client Library (KCL) provided by Amazon under the Amazon Software License.</li>
<li>The KCL builds on top of the Apache 2.0 licensed AWS Java SDK and provides load-balancing, fault-tolerance, checkpointing through the concept of Workers, Checkpoints, and Shard Leases.</li>
<li>The KCL uses DynamoDB to maintain all state.  A DynamoDB table is created in the us-east-1 region (regardless of Kinesis stream region) during KCL initialization for each Kinesis application name.</li>
<li>A single KinesisReceiver can process many shards of a stream by spinning up multiple KinesisRecordProcessor threads.</li>
<li>You never need more KinesisReceivers than the number of shards in your stream as each will spin up at least one KinesisRecordProcessor thread.</li>
<li>Horizontal scaling is achieved by autoscaling additional KinesisReceiver (separate processes) or spinning up new KinesisRecordProcessor threads within each KinesisReceiver - up to the number of current shards for a given stream, of course.  Don't forget to autoscale back down!</li>

### Build
<li>Spark supports a Streaming KinesisReceiver, but it is not included in the default build due to Amazon Software Licensing (ASL) restrictions.</li>
<li>To build with the Kinesis Streaming Receiver and supporting ASL-licensed code, you must run the maven or sbt builds with the **-Pkinesis-asl** profile.</li>
<li>All KinesisReceiver-related code, examples, tests, and artifacts live in **$SPARK_HOME/extras/kinesis-asl/**.</li>
<li>Kinesis-based Spark Applications will need to link to the **spark-streaming-kinesis-asl** artifact that is built when **-Pkinesis-asl** is specified.</li>
<li>_**Note that by linking to this library, you will include [ASL](https://aws.amazon.com/asl/)-licensed code in your Spark package**_.</li>

###Example
<li>To build the Kinesis example, you must run the maven or sbt builds with the **-Pkinesis-asl** profile.</li>
<li>You need to setup a Kinesis stream at one of the valid Kinesis endpoints with 1 or more shards per the following:  http://docs.aws.amazon.com/kinesis/latest/dev/step-one-create-stream.html</li>
<li>Valid Kinesis endpoints can be found here:  http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region</li>
<li>When running **locally**, the example automatically determines the number of threads and KinesisReceivers to spin up based on the number of shards configured for the stream.  Therefore, **local[n]** is not needed when starting the example as with other streaming examples.</li>
<li>While this example could use a single KinesisReceiver which spins up multiple KinesisRecordProcessor threads to process multiple shards, I wanted to demonstrate unioning multiple KinesisReceivers as a single DStream.  (It's a bit confusing in local mode.)</li>
<li>**KinesisWordCountProducerASL** is provided to generate random records into the Kinesis stream for testing.</li>
<li>The example has been configured to immediately replicate incoming stream data to another node by using (StorageLevel.MEMORY_AND_DISK_2)
<li>Spark checkpointing is disabled because the example does not use any stateful or window-based DStream operations such as updateStateByKey and reduceByWindow.  If those operations are introduced, you would need to enable checkpointing or risk losing data in the case of a failure.</li>
<li>Kinesis checkpointing is enabled.  This means that the example will recover from a Kinesis failure.</li>
<li>The example uses InitialPositionInStream.LATEST strategy to pull from the latest tip of the stream if no Kinesis checkpoint info exists.</li>
<li>In our example, **KinesisWordCount** is the Kinesis application name for both the Scala and Java versions.  The use of this application name is described next.</li>

###Deployment and Runtime
<li>A Kinesis application name must be unique for a given account and region.</li>
<li>A DynamoDB table and CloudWatch namespace are created during KCL initialization using this Kinesis application name.  http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-implementation-app.html#kinesis-record-processor-initialization</li>
<li>This DynamoDB table lives in the us-east-1 region regardless of the Kinesis endpoint URL.</li>
<li>Changing the app name or stream name could lead to Kinesis errors as only a single logical application can process a single stream.</li>
<li>If you are seeing errors after changing the app name or stream name, it may be necessary to manually delete the DynamoDB table and start from scratch.</li>
<li>The Kinesis libraries must be present on all worker nodes, as they will need access to the KCL.</li>
<li>The KinesisReceiver uses the DefaultAWSCredentialsProviderChain for AWS credentials which  searches for credentials in the following order of precedence:</br>
1) Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY<br/>
2) Java System Properties - aws.accessKeyId and aws.secretKey<br/>
3) Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs<br/>
4) Instance profile credentials - delivered through the Amazon EC2 metadata service
</li>

###Fault-Tolerance
<li>The combination of Spark Streaming and Kinesis creates 2 different checkpoints that may occur at different intervals.</li>
<li>Checkpointing too frequently against Kinesis will cause excess load on the AWS checkpoint storage layer and may lead to AWS throttling.  The provided example handles this throttling with a random backoff retry strategy.</li>
<li>Upon startup, a KinesisReceiver will begin processing records with sequence numbers greater than the last Kinesis checkpoint sequence number recorded per shard (stored in the DynamoDB table).</li>
<li>If no Kinesis checkpoint info exists, the KinesisReceiver will start either from the oldest record available (InitialPositionInStream.TRIM_HORIZON) or from the latest tip (InitialPostitionInStream.LATEST).  This is configurable.</li>
<li>InitialPositionInStream.LATEST could lead to missed records if data is added to the stream while no KinesisReceivers are running (and no checkpoint info is being stored.)</li>
<li>In production, you'll want to switch to InitialPositionInStream.TRIM_HORIZON which will read up to 24 hours (Kinesis limit) of previous stream data.</li>
<li>InitialPositionInStream.TRIM_HORIZON may lead to duplicate processing of records where the impact is dependent on checkpoint frequency.</li>
<li>Record processing should be idempotent when possible.</li>
<li>A failed or latent KinesisRecordProcessor within the KinesisReceiver will be detected and automatically restarted by the KCL.</li>
<li>If possible, the KinesisReceiver should be shutdown cleanly in order to trigger a final checkpoint of all KinesisRecordProcessors to avoid duplicate record processing.</li>