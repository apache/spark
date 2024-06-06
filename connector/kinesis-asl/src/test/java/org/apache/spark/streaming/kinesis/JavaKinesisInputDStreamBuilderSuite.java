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

package org.apache.spark.streaming.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.streaming.kinesis.KinesisInitialPositions.TrimHorizon;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.LocalJavaStreamingContext;
import org.apache.spark.streaming.Seconds;

public class JavaKinesisInputDStreamBuilderSuite extends LocalJavaStreamingContext {
  /**
   * Basic test to ensure that the KinesisDStream.Builder interface is accessible from Java.
   */
  @Test
  public void testJavaKinesisDStreamBuilder() {
    String streamName = "a-very-nice-stream-name";
    String endpointUrl = "https://kinesis.us-west-2.amazonaws.com";
    String region = "us-west-2";
    KinesisInitialPosition initialPosition = new TrimHorizon();
    String appName = "a-very-nice-kinesis-app";
    Duration checkpointInterval = Seconds.apply(30);
    StorageLevel storageLevel = StorageLevel.MEMORY_ONLY();

    KinesisInputDStream<byte[]> kinesisDStream = KinesisInputDStream.builder()
      .streamingContext(ssc)
      .streamName(streamName)
      .endpointUrl(endpointUrl)
      .regionName(region)
      .initialPosition(initialPosition)
      .checkpointAppName(appName)
      .checkpointInterval(checkpointInterval)
      .storageLevel(storageLevel)
      .build();
    Assertions.assertEquals(streamName, kinesisDStream.streamName());
    Assertions.assertEquals(endpointUrl, kinesisDStream.endpointUrl());
    Assertions.assertEquals(region, kinesisDStream.regionName());
    Assertions.assertEquals(initialPosition.getPosition(),
        kinesisDStream.initialPosition().getPosition());
    Assertions.assertEquals(appName, kinesisDStream.checkpointAppName());
    Assertions.assertEquals(checkpointInterval, kinesisDStream.checkpointInterval());
    Assertions.assertEquals(storageLevel, kinesisDStream._storageLevel());
    ssc.stop();
  }

  /**
   * Test to ensure that the old API for InitialPositionInStream
   * is supported in KinesisDStream.Builder.
   * This test would be removed when we deprecate the KinesisUtils.
   */
  @Test
  public void testJavaKinesisDStreamBuilderOldApi() {
    String streamName = "a-very-nice-stream-name";
    String endpointUrl = "https://kinesis.us-west-2.amazonaws.com";
    String region = "us-west-2";
    String appName = "a-very-nice-kinesis-app";
    Duration checkpointInterval = Seconds.apply(30);
    StorageLevel storageLevel = StorageLevel.MEMORY_ONLY();

    KinesisInputDStream<byte[]> kinesisDStream = KinesisInputDStream.builder()
      .streamingContext(ssc)
      .streamName(streamName)
      .endpointUrl(endpointUrl)
      .regionName(region)
      .initialPositionInStream(InitialPositionInStream.LATEST)
      .checkpointAppName(appName)
      .checkpointInterval(checkpointInterval)
      .storageLevel(storageLevel)
      .build();
    Assertions.assertEquals(streamName, kinesisDStream.streamName());
    Assertions.assertEquals(endpointUrl, kinesisDStream.endpointUrl());
    Assertions.assertEquals(region, kinesisDStream.regionName());
    Assertions.assertEquals(InitialPositionInStream.LATEST,
        kinesisDStream.initialPosition().getPosition());
    Assertions.assertEquals(appName, kinesisDStream.checkpointAppName());
    Assertions.assertEquals(checkpointInterval, kinesisDStream.checkpointInterval());
    Assertions.assertEquals(storageLevel, kinesisDStream._storageLevel());
    ssc.stop();
  }
}
