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
 * Helper class to create Amazon Kinesis Input Stream
 * :: Experimental ::
 */
@Experimental
object KinesisUtils extends Logging {
  /**
   * Create an InputDStream that pulls messages from a Kinesis stream.
   *
   * @param ssc StreamingContext
   * @param appName unique name for your Kinesis app.  Multiple instances of the app pull from
   *   the same stream.  The Kinesis Client Library coordinates all load-balancing and 
   *   failure-recovery.
   * @param stream Kinesis stream name
   * @param endpoint url of Kinesis service (ie. https://kinesis.us-east-1.amazonaws.com)
   *   Available endpoints:  http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region
   * @param checkpointIntervalMillis interval (millis) for Kinesis checkpointing
   * @param initialPositionInStream in the absence of a Kinesis checkpoint info, this is the 
   *   worker's initial starting position in the stream.
   * @return ReceiverInputDStream[Array[Byte]]
   */
  def createStream(
      ssc: StreamingContext,
      appName: String,
      stream: String,
      endpoint: String,
      checkpointIntervalMillis: Long,
      initialPositionInStream: InitialPositionInStream): ReceiverInputDStream[Array[Byte]] = {
    ssc.receiverStream(new KinesisReceiver(appName, stream, endpoint, checkpointIntervalMillis, 
        initialPositionInStream	))
  }

  /**
   * Create a Java-friendly InputDStream that pulls messages from a Kinesis stream.
   *
   * @param jssc Java StreamingContext object
   * @param appName unique name for your Kinesis app.  Multiple instances of the app pull from
   *   the same stream.  The Kinesis Client Library coordinates all load-balancing and 
   *   failure-recovery.
   * @param stream Kinesis stream name
   * @param endpoint url of Kinesis service (ie. https://kinesis.us-east-1.amazonaws.com)
   *   Available endpoints:  http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region
   * @param checkpointIntervalMillis interval (millis) for Kinesis checkpointing
   * @param initialPositionInStream in the absence of a Kinesis checkpoint info, this is the
   *   worker's initial starting position in the stream.
   * @return JavaReceiverInputDStream[Array[Byte]]
   */
  def createStream(
      jssc: JavaStreamingContext, 
      appName: String, 
      stream: String, 
      endpoint: String, 
      checkpointIntervalMillis: Long,
      initialPositionInStream: InitialPositionInStream): JavaReceiverInputDStream[Array[Byte]] = {
    jssc.receiverStream(new KinesisReceiver(appName, stream, endpoint, checkpointIntervalMillis, 
        initialPositionInStream))
  }
}
