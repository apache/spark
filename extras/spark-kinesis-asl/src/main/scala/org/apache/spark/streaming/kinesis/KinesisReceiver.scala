/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.kinesis

import java.net.InetAddress
import java.util.UUID
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import java.nio.ByteBuffer
import org.apache.spark.streaming.util.SystemClock

/**
 * Custom AWS Kinesis-specific implementation of Spark Streaming's Receiver.
 * This implementation relies on the Kinesis Client Library (KCL) Worker as described here:
 * https://github.com/awslabs/amazon-kinesis-client
 * This is a custom receiver used with StreamingContext.receiverStream(Receiver) as described here:
 * http://spark.apache.org/docs/latest/streaming-custom-receivers.html
 * Instances of this class will get shipped to the Spark Streaming Workers to run within a Spark Executor.
 *
 * @param app name
 * @param Kinesis stream name
 * @param endpoint url of Kinesis service
 * @param checkpoint interval (millis) for Kinesis checkpointing (not Spark checkpointing).
 *   See the Kinesis Spark Streaming documentation for more details on the different types of checkpoints.
 * @param in the absence of Kinesis checkpoint info, this is the worker's initial starting position in the stream.
 *   The values are either the beginning of the stream per Kinesis' limit of 24 hours (InitialPositionInStream.TRIM_HORIZON)
 *      or the tip of the stream using InitialPositionInStream.LATEST.
 * @param persistence strategy for RDDs and DStreams.
 */
private[streaming] class KinesisReceiver(
  app: String,
  stream: String,
  endpoint: String,
  checkpointIntervalMillis: Long,
  initialPositionInStream: InitialPositionInStream,
  storageLevel: StorageLevel)
  extends Receiver[Array[Byte]](storageLevel) with Logging { receiver =>

  /**
   *  The lazy val's below will get instantiated in the remote Executor after the closure is shipped to the Spark Worker. 
   *  These are all lazy because they're from third-party Amazon libraries and are not Serializable.
   *  If they're not marked lazy, they will cause NotSerializableExceptions when they're shipped to the Spark Worker.
   */

  /**
   *  workerId is lazy because we want the address of the actual Worker where the code runs - not the Driver's ip address.
   *  This makes a difference when running in a cluster.
   */
  lazy val workerId = InetAddress.getLocalHost.getHostAddress() + ":" + UUID.randomUUID()

  /**
   * This impl uses the DefaultAWSCredentialsProviderChain per the following url:
   *    http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html
   *  and searches for credentials in the following order of precedence:
   * Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
   * Java System Properties - aws.accessKeyId and aws.secretKey
   * Credential profiles file at the default location (~/.aws/credentials) shared by all AWS SDKs and the AWS CLI
   * Instance profile credentials delivered through the Amazon EC2 metadata service
   */
  lazy val credentialsProvider = new DefaultAWSCredentialsProviderChain()

  /** Create a KCL config instance. */
  lazy val KinesisClientLibConfiguration = new KinesisClientLibConfiguration(app, stream, credentialsProvider, workerId)
    .withKinesisEndpoint(endpoint).withInitialPositionInStream(initialPositionInStream).withTaskBackoffTimeMillis(500)

  /**
   *  RecordProcessorFactory creates impls of IRecordProcessor.
   *  IRecordProcessor adapts the KCL to our Spark KinesisReceiver via the IRecordProcessor.processRecords() method.
   *  We're using our custom KinesisRecordProcessor in this case.
   */
  lazy val recordProcessorFactory: IRecordProcessorFactory = new IRecordProcessorFactory {
    override def createProcessor: IRecordProcessor = new KinesisRecordProcessor(receiver, workerId, KinesisUtils.createCheckpointState(checkpointIntervalMillis))
  }

  /**
   * Create a Kinesis Worker.
   * This is the core client abstraction from the Kinesis Client Library (KCL).
   * We pass the RecordProcessorFactory from above as well as the KCL config instance.
   * A Kinesis Worker can process 1..* shards from the given stream - each with its own RecordProcessor.
   */
  lazy val worker: Worker = new Worker(recordProcessorFactory, KinesisClientLibConfiguration);

  /**
   *  This is called when the KinesisReceiver starts and must be non-blocking.
   *  The KCL creates and manages the receiving/processing thread pool through the Worker.run() method.
   */
  override def onStart() {
    logInfo(s"Starting receiver with workerId $workerId")
    worker.run()
  }

  /**
   *  This is called when the KinesisReceiver stops.
   *  The KCL worker.shutdown() method stops the receiving/processing threads.
   *  The KCL will do its best to drain and checkpoint any in-flight records upon shutdown.
   */
  override def onStop() {
    logInfo(s"Shutting down receiver with workerId $workerId")
    worker.shutdown()
  }
}
