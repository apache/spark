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

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream


/**
 * Facade to create the Scala-based or Java-based streams.
 * Also, contains a reusable utility methods.
 * :: Experimental ::
 */
@Experimental
object KinesisUtils extends Logging {
  /**
   * Create an InputDStream that pulls messages from a Kinesis stream.
   *
   * @param StreamingContext object
   * @param appName Kinesis Application Name.  Kinesis Apps are mapped to Kinesis Streams 
   *   by the Kinesis Client Library.  If you change the App name or Stream name, 
   *   the KCL will throw errors.
   * @param stream Kinesis Stream Name
   * @param endpoint url of Kinesis service
   * @param checkpoint interval (millis) for Kinesis checkpointing (not Spark checkpointing).
   * See the Kinesis Spark Streaming documentation for more details on the different types 
   *   of checkpoints.
   * @param initialPositionInStream in the absence of Kinesis checkpoint info, this is the 
   *   worker's initial starting position in the stream.
   * The values are either the beginning of the stream per Kinesis' limit of 24 hours 
   *   (InitialPositionInStream.TRIM_HORIZON) or the tip of the stream 
   *   (InitialPositionInStream.LATEST).
   * The default is TRIM_HORIZON to avoid potential data loss.  However, this presents the risk 
   *   of processing records more than once.
   * @param storageLevel The default is StorageLevel.MEMORY_AND_DISK_2 which replicates in-memory 
   *   and on-disk to 2 nodes total (primary and secondary)
   *
   * @return ReceiverInputDStream[Array[Byte]]
   */
  def createStream(
      ssc: StreamingContext,
      appName: String,
      stream: String,
      endpoint: String,
      checkpointIntervalMillis: Long,
      initialPositionInStream: InitialPositionInStream,
      storageLevel: StorageLevel): ReceiverInputDStream[Array[Byte]] = {
    ssc.receiverStream(new KinesisReceiver(appName, stream, endpoint, checkpointIntervalMillis, 
        initialPositionInStream, storageLevel))
  }

  /**
   * Create a Java-friendly InputDStream that pulls messages from a Kinesis stream.
   *
   * @param JavaStreamingContext object
   * @param appName Kinesis Application Name.  Kinesis Apps are mapped to Kinesis Streams 
   *   by the Kinesis Client Library.  If you change the App name or Stream name, 
   *   the KCL will throw errors.
   * @param stream Kinesis Stream Name
   * @param endpoint url of Kinesis service
   * @param checkpoint interval (millis) for Kinesis checkpointing (not Spark checkpointing).
   * See the Kinesis Spark Streaming documentation for more details on the different types 
   *   of checkpoints.
   * @param initialPositionInStream in the absence of Kinesis checkpoint info, this is the 
   *   worker's initial starting position in the stream.
   * The values are either the beginning of the stream per Kinesis' limit of 24 hours 
   *   (InitialPositionInStream.TRIM_HORIZON) or the tip of the stream 
   *   (InitialPositionInStream.LATEST).
   * The default is TRIM_HORIZON to avoid potential data loss.  However, this presents the risk 
   *   of processing records more than once.
   * @param storageLevel The default is StorageLevel.MEMORY_AND_DISK_2 which replicates in-memory 
   *   and on-disk to 2 nodes total (primary and secondary)
   *
   * @return JavaReceiverInputDStream[Array[Byte]]
   */
  def createStream(
      jssc: JavaStreamingContext, 
      appName: String, 
      stream: String, 
      endpoint: String, 
      checkpointIntervalMillis: Long, 
      initialPositionInStream: InitialPositionInStream, 
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    jssc.receiverStream(new KinesisReceiver(appName, stream, endpoint, checkpointIntervalMillis, 
        initialPositionInStream, storageLevel))
  }
}
