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

import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.LocalJavaStreamingContext;

public class JavaKinesisInputDStreamBuilderSuite extends LocalJavaStreamingContext {
  /**
   * Basic test to ensure that the KinesisDStream.Builder interface is accessible from Java.
   */
  @Test
  public void testJavaKinesisDStreamBuilder() {
    String streamName = "a-very-nice-stream-name";
    String endpointUrl = "https://kinesis.us-west-2.amazonaws.com";
    String region = "us-west-2";
    InitialPositionInStream initialPosition = InitialPositionInStream.TRIM_HORIZON;
    String appName = "a-very-nice-kinesis-app";
    Duration checkpointInterval = Seconds.apply(30);
    StorageLevel storageLevel = StorageLevel.MEMORY_ONLY();

    KinesisInputDStream<byte[]> kinesisDStream = KinesisInputDStream.builder()
      .streamingContext(ssc)
      .streamName(streamName)
      .endpointUrl(endpointUrl)
      .regionName(region)
      .initialPositionInStream(initialPosition)
      .checkpointAppName(appName)
      .checkpointInterval(checkpointInterval)
      .storageLevel(storageLevel)
      .build();
    assert(kinesisDStream.streamName() == streamName);
    assert(kinesisDStream.endpointUrl() == endpointUrl);
    assert(kinesisDStream.regionName() == region);
    assert(kinesisDStream.initialPositionInStream() == initialPosition);
    assert(kinesisDStream.checkpointAppName() == appName);
    assert(kinesisDStream.checkpointInterval() == checkpointInterval);
    assert(kinesisDStream._storageLevel() == storageLevel);
    ssc.stop();
  }
}
