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

import scala.reflect.ClassTag

import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.Record

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object KinesisUtils {
  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note: The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   * on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   * gets the AWS credentials.
   *
   * @param ssc StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param messageHandler A custom message handler that can generate a generic output from a
   *                       Kinesis `Record`, which contains both message data, and metadata.
   */
  def createStream[T: ClassTag](
      ssc: StreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel,
      messageHandler: Record => T): ReceiverInputDStream[T] = {
    val cleanedHandler = ssc.sc.clean(messageHandler)
    // Setting scope to override receiver stream's scope of "receiver stream"
    ssc.withNamedScope("kinesis stream") {
      new KinesisInputDStream[T](ssc, streamName, endpointUrl, validateRegion(regionName),
        initialPositionInStream, kinesisAppName, checkpointInterval, storageLevel,
        cleanedHandler, None)
    }
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note:
   *  The given AWS credentials will get saved in DStream checkpoints if checkpointing
   *  is enabled. Make sure that your checkpoint directory is secure.
   *
   * @param ssc StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param messageHandler A custom message handler that can generate a generic output from a
   *                       Kinesis `Record`, which contains both message data, and metadata.
   * @param awsAccessKeyId  AWS AccessKeyId (if null, will use DefaultAWSCredentialsProviderChain)
   * @param awsSecretKey  AWS SecretKey (if null, will use DefaultAWSCredentialsProviderChain)
   */
  // scalastyle:off
  def createStream[T: ClassTag](
      ssc: StreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel,
      messageHandler: Record => T,
      awsAccessKeyId: String,
      awsSecretKey: String): ReceiverInputDStream[T] = {
    // scalastyle:on
    val cleanedHandler = ssc.sc.clean(messageHandler)
    ssc.withNamedScope("kinesis stream") {
      new KinesisInputDStream[T](ssc, streamName, endpointUrl, validateRegion(regionName),
        initialPositionInStream, kinesisAppName, checkpointInterval, storageLevel,
        cleanedHandler, Some(SerializableAWSCredentials(awsAccessKeyId, awsSecretKey)))
    }
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note: The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   * on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   * gets the AWS credentials.
   *
   * @param ssc StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   */
  def createStream(
      ssc: StreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel): ReceiverInputDStream[Array[Byte]] = {
    // Setting scope to override receiver stream's scope of "receiver stream"
    ssc.withNamedScope("kinesis stream") {
      new KinesisInputDStream[Array[Byte]](ssc, streamName, endpointUrl, validateRegion(regionName),
        initialPositionInStream, kinesisAppName, checkpointInterval, storageLevel,
        defaultMessageHandler, None)
    }
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note:
   *  The given AWS credentials will get saved in DStream checkpoints if checkpointing
   *  is enabled. Make sure that your checkpoint directory is secure.
   *
   * @param ssc StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param awsAccessKeyId  AWS AccessKeyId (if null, will use DefaultAWSCredentialsProviderChain)
   * @param awsSecretKey  AWS SecretKey (if null, will use DefaultAWSCredentialsProviderChain)
   */
  def createStream(
      ssc: StreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel,
      awsAccessKeyId: String,
      awsSecretKey: String): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("kinesis stream") {
      new KinesisInputDStream[Array[Byte]](ssc, streamName, endpointUrl, validateRegion(regionName),
        initialPositionInStream, kinesisAppName, checkpointInterval, storageLevel,
        defaultMessageHandler, Some(SerializableAWSCredentials(awsAccessKeyId, awsSecretKey)))
    }
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note: The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   * on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   * gets the AWS credentials.
   *
   * @param jssc Java StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param messageHandler A custom message handler that can generate a generic output from a
   *                       Kinesis `Record`, which contains both message data, and metadata.
   * @param recordClass Class of the records in DStream
   */
  def createStream[T](
      jssc: JavaStreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel,
      messageHandler: JFunction[Record, T],
      recordClass: Class[T]): JavaReceiverInputDStream[T] = {
    implicit val recordCmt: ClassTag[T] = ClassTag(recordClass)
    val cleanedHandler = jssc.sparkContext.clean(messageHandler.call(_))
    createStream[T](jssc.ssc, kinesisAppName, streamName, endpointUrl, regionName,
      initialPositionInStream, checkpointInterval, storageLevel, cleanedHandler)
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note:
   * The given AWS credentials will get saved in DStream checkpoints if checkpointing
   * is enabled. Make sure that your checkpoint directory is secure.
   *
   * @param jssc Java StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param messageHandler A custom message handler that can generate a generic output from a
   *                       Kinesis `Record`, which contains both message data, and metadata.
   * @param recordClass Class of the records in DStream
   * @param awsAccessKeyId  AWS AccessKeyId (if null, will use DefaultAWSCredentialsProviderChain)
   * @param awsSecretKey  AWS SecretKey (if null, will use DefaultAWSCredentialsProviderChain)
   */
  // scalastyle:off
  def createStream[T](
      jssc: JavaStreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel,
      messageHandler: JFunction[Record, T],
      recordClass: Class[T],
      awsAccessKeyId: String,
      awsSecretKey: String): JavaReceiverInputDStream[T] = {
    // scalastyle:on
    implicit val recordCmt: ClassTag[T] = ClassTag(recordClass)
    val cleanedHandler = jssc.sparkContext.clean(messageHandler.call(_))
    createStream[T](jssc.ssc, kinesisAppName, streamName, endpointUrl, regionName,
      initialPositionInStream, checkpointInterval, storageLevel, cleanedHandler,
      awsAccessKeyId, awsSecretKey)
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note: The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   * on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   * gets the AWS credentials.
   *
   * @param jssc Java StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   */
  def createStream(
      jssc: JavaStreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[Array[Byte]] = {
    createStream[Array[Byte]](jssc.ssc, kinesisAppName, streamName, endpointUrl, regionName,
      initialPositionInStream, checkpointInterval, storageLevel, defaultMessageHandler(_))
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note:
   * The given AWS credentials will get saved in DStream checkpoints if checkpointing
   * is enabled. Make sure that your checkpoint directory is secure.
   *
   * @param jssc Java StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param awsAccessKeyId  AWS AccessKeyId (if null, will use DefaultAWSCredentialsProviderChain)
   * @param awsSecretKey  AWS SecretKey (if null, will use DefaultAWSCredentialsProviderChain)
   */
  def createStream(
      jssc: JavaStreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel,
      awsAccessKeyId: String,
      awsSecretKey: String): JavaReceiverInputDStream[Array[Byte]] = {
    createStream[Array[Byte]](jssc.ssc, kinesisAppName, streamName, endpointUrl, regionName,
      initialPositionInStream, checkpointInterval, storageLevel,
      defaultMessageHandler(_), awsAccessKeyId, awsSecretKey)
  }

  private def getRegionByEndpoint(endpointUrl: String): String = {
    RegionUtils.getRegionByEndpoint(endpointUrl).getName()
  }

  private def validateRegion(regionName: String): String = {
    Option(RegionUtils.getRegion(regionName)).map { _.getName }.getOrElse {
      throw new IllegalArgumentException(s"Region name '$regionName' is not valid")
    }
  }

  private[kinesis] def defaultMessageHandler(record: Record): Array[Byte] = {
    if (record == null) return null
    val byteBuffer = record.getData()
    val byteArray = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(byteArray)
    byteArray
  }
}

/**
 * This is a helper class that wraps the methods in KinesisUtils into more Python-friendly class and
 * function so that it can be easily instantiated and called from Python's KinesisUtils.
 */
private class KinesisUtilsPythonHelper {

  def getInitialPositionInStream(initialPositionInStream: Int): InitialPositionInStream = {
    initialPositionInStream match {
      case 0 => InitialPositionInStream.LATEST
      case 1 => InitialPositionInStream.TRIM_HORIZON
      case _ => throw new IllegalArgumentException(
        "Illegal InitialPositionInStream. Please use " +
          "InitialPositionInStream.LATEST or InitialPositionInStream.TRIM_HORIZON")
    }
  }

  def createStream(
      jssc: JavaStreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: Int,
      checkpointInterval: Duration,
      storageLevel: StorageLevel,
      awsAccessKeyId: String,
      awsSecretKey: String
      ): JavaReceiverInputDStream[Array[Byte]] = {
    if (awsAccessKeyId == null && awsSecretKey != null) {
      throw new IllegalArgumentException("awsSecretKey is set but awsAccessKeyId is null")
    }
    if (awsAccessKeyId != null && awsSecretKey == null) {
      throw new IllegalArgumentException("awsAccessKeyId is set but awsSecretKey is null")
    }
    if (awsAccessKeyId == null && awsSecretKey == null) {
      KinesisUtils.createStream(jssc, kinesisAppName, streamName, endpointUrl, regionName,
        getInitialPositionInStream(initialPositionInStream), checkpointInterval, storageLevel)
    } else {
      KinesisUtils.createStream(jssc, kinesisAppName, streamName, endpointUrl, regionName,
        getInitialPositionInStream(initialPositionInStream), checkpointInterval, storageLevel,
        awsAccessKeyId, awsSecretKey)
    }
  }

}
