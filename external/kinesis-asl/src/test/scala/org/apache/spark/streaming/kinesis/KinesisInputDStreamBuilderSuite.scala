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

import java.lang.IllegalArgumentException

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.scalatest.BeforeAndAfterEach
import org.scalatest.mock.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, TestSuiteBase}

class KinesisInputDStreamBuilderSuite extends TestSuiteBase with BeforeAndAfterEach
   with MockitoSugar {
  import KinesisInputDStream._

  private val ssc = new StreamingContext(conf, batchDuration)
  private val streamName = "a-very-nice-kinesis-stream-name"
  private val checkpointAppName = "a-very-nice-kcl-app-name"
  private def baseBuilder = KinesisInputDStream.builder
  private def builder = baseBuilder.streamingContext(ssc)
    .streamName(streamName)
    .checkpointAppName(checkpointAppName)

  override def afterAll(): Unit = {
    ssc.stop()
  }

  test("should raise an exception if the StreamingContext is missing") {
    intercept[IllegalArgumentException] {
      baseBuilder.streamName(streamName).checkpointAppName(checkpointAppName).build()
    }
  }

  test("should raise an exception if the stream name is missing") {
    intercept[IllegalArgumentException] {
      baseBuilder.streamingContext(ssc).checkpointAppName(checkpointAppName).build()
    }
  }

  test("should raise an exception if the checkpoint app name is missing") {
    intercept[IllegalArgumentException] {
      baseBuilder.streamingContext(ssc).streamName(streamName).build()
    }
  }

  test("should propagate required values to KinesisInputDStream") {
    val dstream = builder.build()
    assert(dstream.context == ssc)
    assert(dstream.streamName == streamName)
    assert(dstream.checkpointAppName == checkpointAppName)
  }

  test("should propagate default values to KinesisInputDStream") {
    val dstream = builder.build()
    assert(dstream.endpointUrl == DEFAULT_KINESIS_ENDPOINT_URL)
    assert(dstream.regionName == DEFAULT_KINESIS_REGION_NAME)
    assert(dstream.initialPositionInStream == DEFAULT_INITIAL_POSITION_IN_STREAM)
    assert(dstream.checkpointInterval == batchDuration)
    assert(dstream._storageLevel == DEFAULT_STORAGE_LEVEL)
    assert(dstream.kinesisCreds == DefaultCredentials)
    assert(dstream.dynamoDBCreds == None)
    assert(dstream.cloudWatchCreds == None)
  }

  test("should propagate custom non-auth values to KinesisInputDStream") {
    val customEndpointUrl = "https://kinesis.us-west-2.amazonaws.com"
    val customRegion = "us-west-2"
    val customInitialPosition = InitialPositionInStream.TRIM_HORIZON
    val customAppName = "a-very-nice-kinesis-app"
    val customCheckpointInterval = Seconds(30)
    val customStorageLevel = StorageLevel.MEMORY_ONLY
    val customKinesisCreds = mock[SparkAWSCredentials]
    val customDynamoDBCreds = mock[SparkAWSCredentials]
    val customCloudWatchCreds = mock[SparkAWSCredentials]

    val dstream = builder
      .endpointUrl(customEndpointUrl)
      .regionName(customRegion)
      .initialPositionInStream(customInitialPosition)
      .checkpointAppName(customAppName)
      .checkpointInterval(customCheckpointInterval)
      .storageLevel(customStorageLevel)
      .kinesisCredentials(customKinesisCreds)
      .dynamoDBCredentials(customDynamoDBCreds)
      .cloudWatchCredentials(customCloudWatchCreds)
      .build()
    assert(dstream.endpointUrl == customEndpointUrl)
    assert(dstream.regionName == customRegion)
    assert(dstream.initialPositionInStream == customInitialPosition)
    assert(dstream.checkpointAppName == customAppName)
    assert(dstream.checkpointInterval == customCheckpointInterval)
    assert(dstream._storageLevel == customStorageLevel)
    assert(dstream.kinesisCreds == customKinesisCreds)
    assert(dstream.dynamoDBCreds == Option(customDynamoDBCreds))
    assert(dstream.cloudWatchCreds == Option(customCloudWatchCreds))
  }
}
