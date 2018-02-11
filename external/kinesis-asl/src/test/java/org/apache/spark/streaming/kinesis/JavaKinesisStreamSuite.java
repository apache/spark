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

import com.amazonaws.services.kinesis.model.Record;
import org.junit.Test;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.LocalJavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

/**
 * Demonstrate the use of the KinesisUtils Java API
 */
public class JavaKinesisStreamSuite extends LocalJavaStreamingContext {
  @Test
  public void testKinesisStream() {
    String dummyEndpointUrl = KinesisTestUtils.defaultEndpointUrl();
    String dummyRegionName = KinesisTestUtils.getRegionNameByEndpoint(dummyEndpointUrl);

    // Tests the API, does not actually test data receiving
    JavaDStream<byte[]> kinesisStream = KinesisUtils.createStream(ssc, "myAppName", "mySparkStream",
        dummyEndpointUrl, dummyRegionName, InitialPositionInStream.LATEST, new Duration(2000),
        StorageLevel.MEMORY_AND_DISK_2());
    ssc.stop();
  }

  @Test
  public void testAwsCreds() {
    String dummyEndpointUrl = KinesisTestUtils.defaultEndpointUrl();
    String dummyRegionName = KinesisTestUtils.getRegionNameByEndpoint(dummyEndpointUrl);

    // Tests the API, does not actually test data receiving
    JavaDStream<byte[]> kinesisStream = KinesisUtils.createStream(ssc, "myAppName", "mySparkStream",
        dummyEndpointUrl, dummyRegionName, InitialPositionInStream.LATEST, new Duration(2000),
        StorageLevel.MEMORY_AND_DISK_2(), "fakeAccessKey", "fakeSecretKey");
    ssc.stop();
  }

  private static Function<Record, String> handler = new Function<Record, String>() {
    @Override
    public String call(Record record) {
      return record.getPartitionKey() + "-" + record.getSequenceNumber();
    }
  };

  @Test
  public void testCustomHandler() {
    // Tests the API, does not actually test data receiving
    JavaDStream<String> kinesisStream = KinesisUtils.createStream(ssc, "testApp", "mySparkStream",
        "https://kinesis.us-west-2.amazonaws.com", "us-west-2", InitialPositionInStream.LATEST,
        new Duration(2000), StorageLevel.MEMORY_AND_DISK_2(), handler, String.class);

    ssc.stop();
  }

  @Test
  public void testCustomHandlerAwsCreds() {
    // Tests the API, does not actually test data receiving
    JavaDStream<String> kinesisStream = KinesisUtils.createStream(ssc, "testApp", "mySparkStream",
        "https://kinesis.us-west-2.amazonaws.com", "us-west-2", InitialPositionInStream.LATEST,
        new Duration(2000), StorageLevel.MEMORY_AND_DISK_2(), handler, String.class,
        "fakeAccessKey", "fakeSecretKey");

    ssc.stop();
  }

  @Test
  public void testCustomHandlerAwsStsCreds() {
    // Tests the API, does not actually test data receiving
    JavaDStream<String> kinesisStream = KinesisUtils.createStream(ssc, "testApp", "mySparkStream",
        "https://kinesis.us-west-2.amazonaws.com", "us-west-2", InitialPositionInStream.LATEST,
        new Duration(2000), StorageLevel.MEMORY_AND_DISK_2(), handler, String.class,
        "fakeAccessKey", "fakeSecretKey", "fakeSTSRoleArn", "fakeSTSSessionName",
        "fakeSTSExternalId");

    ssc.stop();
  }
}
