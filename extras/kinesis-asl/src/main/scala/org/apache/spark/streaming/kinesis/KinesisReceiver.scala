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
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.receiver.Receiver

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker

/**
 * Custom AWS Kinesis-specific implementation of Spark Streaming's Receiver.
 * This implementation relies on the Kinesis Client Library (KCL) Worker as described here:
 * https://github.com/awslabs/amazon-kinesis-client
 * This is a custom receiver used with StreamingContext.receiverStream(Receiver) 
 *   as described here:
 *     http://spark.apache.org/docs/latest/streaming-custom-receivers.html
 * Instances of this class will get shipped to the Spark Streaming Workers 
 *   to run within a Spark Executor.
 *
 * @param appName  Kinesis application name. Kinesis Apps are mapped to Kinesis Streams
 *                 by the Kinesis Client Library.  If you change the App name or Stream name,
 *                 the KCL will throw errors.  This usually requires deleting the backing  
 *                 DynamoDB table with the same name this Kinesis application.
 * @param streamName   Kinesis stream name
 * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
 * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
 *                            See the Kinesis Spark Streaming documentation for more
 *                            details on the different types of checkpoints.
 * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
 *                                 worker's initial starting position in the stream.
 *                                 The values are either the beginning of the stream
 *                                 per Kinesis' limit of 24 hours
 *                                 (InitialPositionInStream.TRIM_HORIZON) or
 *                                 the tip of the stream (InitialPositionInStream.LATEST).
 * @param storageLevel Storage level to use for storing the received objects
 *
 * @return ReceiverInputDStream[Array[Byte]]   
 */
private[kinesis] class KinesisReceiver(
    appName: String,
    streamName: String,
    endpointUrl: String,
    checkpointInterval: Duration,
    initialPositionInStream: InitialPositionInStream,
    storageLevel: StorageLevel)
  extends Receiver[Array[Byte]](storageLevel) with Logging { receiver =>

  /*
   * The following vars are built in the onStart() method which executes in the Spark Worker after
   *   this code is serialized and shipped remotely.
   */

  /*
   *  workerId should be based on the ip address of the actual Spark Worker where this code runs
   *   (not the Driver's ip address.)
   */
  var workerId: String = null

  /*
   * This impl uses the DefaultAWSCredentialsProviderChain and searches for credentials 
   *   in the following order of precedence:
   * Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
   * Java System Properties - aws.accessKeyId and aws.secretKey
   * Credential profiles file at the default location (~/.aws/credentials) shared by all 
   *   AWS SDKs and the AWS CLI
   * Instance profile credentials delivered through the Amazon EC2 metadata service
   */
  var credentialsProvider: AWSCredentialsProvider = null

  /* KCL config instance. */
  var kinesisClientLibConfiguration: KinesisClientLibConfiguration = null

  /*
   *  RecordProcessorFactory creates impls of IRecordProcessor.
   *  IRecordProcessor adapts the KCL to our Spark KinesisReceiver via the 
   *    IRecordProcessor.processRecords() method.
   *  We're using our custom KinesisRecordProcessor in this case.
   */
  var recordProcessorFactory: IRecordProcessorFactory = null

  /*
   * Create a Kinesis Worker.
   * This is the core client abstraction from the Kinesis Client Library (KCL).
   * We pass the RecordProcessorFactory from above as well as the KCL config instance.
   * A Kinesis Worker can process 1..* shards from the given stream - each with its 
   *   own RecordProcessor.
   */
  var worker: Worker = null

  /**
   *  This is called when the KinesisReceiver starts and must be non-blocking.
   *  The KCL creates and manages the receiving/processing thread pool through the Worker.run() 
   *    method.
   */
  override def onStart() {
    workerId = InetAddress.getLocalHost.getHostAddress() + ":" + UUID.randomUUID()
    credentialsProvider = new DefaultAWSCredentialsProviderChain()
    kinesisClientLibConfiguration = new KinesisClientLibConfiguration(appName, streamName,
      credentialsProvider, workerId).withKinesisEndpoint(endpointUrl)
      .withInitialPositionInStream(initialPositionInStream).withTaskBackoffTimeMillis(500)
    recordProcessorFactory = new IRecordProcessorFactory {
      override def createProcessor: IRecordProcessor = new KinesisRecordProcessor(receiver,
        workerId, new KinesisCheckpointState(checkpointInterval))
    }
    worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration)
    worker.run()
    logInfo(s"Started receiver with workerId $workerId")
  }

  /**
   *  This is called when the KinesisReceiver stops.
   *  The KCL worker.shutdown() method stops the receiving/processing threads.
   *  The KCL will do its best to drain and checkpoint any in-flight records upon shutdown.
   */
  override def onStop() {
    worker.shutdown()
    logInfo(s"Shut down receiver with workerId $workerId")
    workerId = null
    credentialsProvider = null
    kinesisClientLibConfiguration = null
    recordProcessorFactory = null
    worker = null
  }
}
