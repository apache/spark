---
layout: global
title: Spark Streaming Kinesis Receiver
---

### Kinesis
Build notes:
<li>Spark supports a Kinesis Streaming Receiver which is not included in the default build due to licensing restrictions.</li>
<li>_**Note that by embedding this library you will include [ASL](https://aws.amazon.com/asl/)-licensed code in your Spark package**_.</li>
<li>The Spark Kinesis Streaming Receiver source code, examples, tests, and artifacts live in $SPARK_HOME/extras/kinesis-asl.</li>
<li>sbt and maven builds:  must enable the `-Pkinesis-asl` profile.</li>
<li>To build the examples JAR, you must run the maven build with `mvn -Pkinesis-asl package` to create the runnable Kinesis example jar.</li>
<li>Applications will need to link to the 'kinesis-asl` artifact.</li>

Deployment and runtime notes:
<li>Each shard of a stream is processed by one or more KinesisReceiver's managed by the Kinesis Client Library (KCL) Worker.</li>
<li>Said differently, a single KinesisReceiver can process many shards of a stream.</li>
<li>You never need more KinesisReceivers than the number of shards in your stream.</li>
<li>The Kinesis assembly jar must also be present on all worker nodes, as they will need access to the Kinesis Client Library.</li>
<li>/tmp/checkpoint is a valid and accessible directory on all workers (or locally if running in local mode)</li>
<li>This code uses the DefaultAWSCredentialsProviderChain and searches for credentials in the following order of precedence:<br/>
    1) Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY<br/>
    2) Java System Properties - aws.accessKeyId and aws.secretKey<br/>
    3) Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs<br/>
    4) Instance profile credentials - delivered through the Amazon EC2 metadata service<br/>
</li>
<li>You need to setup a Kinesis stream with 1 or more shards per the following:<br/>
 http://docs.aws.amazon.com/kinesis/latest/dev/step-one-create-stream.html</li>
<li>Valid Kinesis endpoint urls can be found here:  Valid enpoint urls:  http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region</li>
<li>When you first start up the KinesisReceiver, the Kinesis Client Library (KCL) needs ~30s to establish connectivity with the AWS Kinesis service,
retrieve any checkpoint data, and negotiate with other KCL's reading from the same stream.</li>
<li>During testing, I noticed varying degrees of delays while retrieving records from Kinesis depending on which coffee shop in San Francisco I was working.
The input and output data eventually matched, but sometimes after an unusually long time.</li>
<li>Be careful when changing the app name.  Kinesis maintains a mapping table in DynamoDB based on this app name (http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-implementation-app.html#kinesis-record-processor-initialization).  
Changing the app name could lead to Kinesis errors as only 1 logical application can process a stream.  In order to start fresh, 
it's always best to delete the DynamoDB table that matches your app name.</li>

Failure recovery notes:
<li>The combination of Spark Streaming and Kinesis creates 3 different checkpoints as follows:<br/>
  1) RDD data checkpoint (Spark Streaming) - frequency is configurable with DStream.checkpoint(Duration)<br/>
  2) RDD metadata checkpoint (Spark Streaming) - frequency is every DStream batch<br/>
  3) Kinesis checkpointing (Kinesis) - frequency is controlled by the developer calling ICheckpointer.checkpoint() directly<br/>
</li>
<li>During testing, if you see the same data being read from the stream twice, it's likely due to the Kinesis checkpoints not being written.</li>
<li>Checkpointing too frequently will cause excess load on the AWS checkpoint storage layer and may lead to AWS throttling</li>
<li>Upon startup, a KinesisReceiver will begin processing records with sequence numbers greater than the last checkpoint sequence number recorded per shard.</li>
<li>If no checkpoint info exists, the worker will start either from the oldest record available (InitialPositionInStream.TRIM_HORIZON)
or from the tip/latest (InitialPostitionInStream.LATEST).  This is configurable.</li>
<li>When pulling from the stream tip (InitialPositionInStream.LATEST), only new stream data will be picked up after the KinesisReceiver starts.</li>
<li>InitialPositionInStream.LATEST could lead to missed records if data is added to the stream while no KinesisReceivers are running.</li>
<li>In production, you'll want to switch to InitialPositionInStream.TRIM_HORIZON which will read up to 24 hours (Kinesis limit) of previous stream data
depending on the checkpoint frequency.</li>
<li>InitialPositionInStream.TRIM_HORIZON may lead to duplicate processing of records depending on the checkpoint frequency.</li>
<li>Record processing should be idempotent when possible.</li>
<li>Failed or latent KinesisReceivers will be detected and automatically shutdown/load-balanced by the KCL.</li>
<li>If possible, explicitly shutdown the worker if a failure occurs.</li>

Example KinesisWordCount (and JavaKinesisWordCount) notes:
<li>These examples automatically determine the number of threads to run locally based on the number of shards for the stream.</li>
<li>These examples automatically determine the number of KinesisReceivers/InputDStreams to create based on the number of shards for the stream.</li>
<li>The KinesisWordCountProducer will generate random data to put onto the Kinesis stream for testing.</li>
