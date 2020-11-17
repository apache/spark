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

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}

/**
 * This is a helper class that wraps the methods in KinesisUtils into more Python-friendly class and
 * function so that it can be easily instantiated and called from Python's KinesisUtils.
 */
private class KinesisUtilsPythonHelper {

  // scalastyle:off
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
      awsSecretKey: String,
      stsAssumeRoleArn: String,
      stsSessionName: String,
      stsExternalId: String): JavaReceiverInputDStream[Array[Byte]] = {
    // scalastyle:on
    if (!(stsAssumeRoleArn != null && stsSessionName != null && stsExternalId != null)
        && !(stsAssumeRoleArn == null && stsSessionName == null && stsExternalId == null)) {
      throw new IllegalArgumentException("stsAssumeRoleArn, stsSessionName, and stsExternalId " +
        "must all be defined or all be null")
    }
    if (awsAccessKeyId == null && awsSecretKey != null) {
      throw new IllegalArgumentException("awsSecretKey is set but awsAccessKeyId is null")
    }
    if (awsAccessKeyId != null && awsSecretKey == null) {
      throw new IllegalArgumentException("awsAccessKeyId is set but awsSecretKey is null")
    }

    val kinesisInitialPosition = initialPositionInStream match {
      case 0 => InitialPositionInStream.LATEST
      case 1 => InitialPositionInStream.TRIM_HORIZON
      case _ => throw new IllegalArgumentException(
        "Illegal InitialPositionInStream. Please use " +
          "InitialPositionInStream.LATEST or InitialPositionInStream.TRIM_HORIZON")
    }

    val builder = KinesisInputDStream.builder.
      streamingContext(jssc).
      checkpointAppName(kinesisAppName).
      streamName(streamName).
      endpointUrl(endpointUrl).
      regionName(regionName).
      initialPosition(KinesisInitialPositions.fromKinesisInitialPosition(kinesisInitialPosition)).
      checkpointInterval(checkpointInterval).
      storageLevel(storageLevel)

    if (stsAssumeRoleArn != null && stsSessionName != null && stsExternalId != null) {
      val kinesisCredsProvider = STSCredentials(
        stsAssumeRoleArn, stsSessionName, Option(stsExternalId),
        BasicCredentials(awsAccessKeyId, awsSecretKey))
      builder.
        kinesisCredentials(kinesisCredsProvider).
        buildWithMessageHandler(KinesisInputDStream.defaultMessageHandler)
    } else {
      if (awsAccessKeyId == null && awsSecretKey == null) {
        builder.build()
      } else {
        builder.kinesisCredentials(BasicCredentials(awsAccessKeyId, awsSecretKey)).build()
      }
    }
  }

}
