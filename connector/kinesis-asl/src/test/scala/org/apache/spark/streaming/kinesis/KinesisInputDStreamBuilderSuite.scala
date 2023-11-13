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

import java.util.Calendar

import scala.jdk.CollectionConverters._

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext, TestSuiteBase}
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.{AtTimestamp, TrimHorizon}

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
    try {
      ssc.stop()
    } finally {
      super.afterAll()
    }
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
    assert(dstream.initialPosition == DEFAULT_INITIAL_POSITION)
    assert(dstream.checkpointInterval == batchDuration)
    assert(dstream._storageLevel == DEFAULT_STORAGE_LEVEL)
    assert(dstream.kinesisCreds == DefaultCredentials)
    assert(dstream.dynamoDBCreds == None)
    assert(dstream.cloudWatchCreds == None)
    assert(dstream.metricsLevel == DEFAULT_METRICS_LEVEL)
    assert(dstream.metricsEnabledDimensions == DEFAULT_METRICS_ENABLED_DIMENSIONS)
  }

  test("should propagate custom non-auth values to KinesisInputDStream") {
    val customEndpointUrl = "https://kinesis.us-west-2.amazonaws.com"
    val customRegion = "us-west-2"
    val customInitialPosition = new TrimHorizon()
    val customAppName = "a-very-nice-kinesis-app"
    val customCheckpointInterval = Seconds(30)
    val customStorageLevel = StorageLevel.MEMORY_ONLY
    val customKinesisCreds = mock[SparkAWSCredentials]
    val customDynamoDBCreds = mock[SparkAWSCredentials]
    val customCloudWatchCreds = mock[SparkAWSCredentials]
    val customMetricsLevel = MetricsLevel.NONE
    val customMetricsEnabledDimensions =
      KinesisClientLibConfiguration.METRICS_ALWAYS_ENABLED_DIMENSIONS.asScala.toSet

    val dstream = builder
      .endpointUrl(customEndpointUrl)
      .regionName(customRegion)
      .initialPosition(customInitialPosition)
      .checkpointAppName(customAppName)
      .checkpointInterval(customCheckpointInterval)
      .storageLevel(customStorageLevel)
      .kinesisCredentials(customKinesisCreds)
      .dynamoDBCredentials(customDynamoDBCreds)
      .cloudWatchCredentials(customCloudWatchCreds)
      .metricsLevel(customMetricsLevel)
      .metricsEnabledDimensions(customMetricsEnabledDimensions)
      .build()
    assert(dstream.endpointUrl == customEndpointUrl)
    assert(dstream.regionName == customRegion)
    assert(dstream.initialPosition == customInitialPosition)
    assert(dstream.checkpointAppName == customAppName)
    assert(dstream.checkpointInterval == customCheckpointInterval)
    assert(dstream._storageLevel == customStorageLevel)
    assert(dstream.kinesisCreds == customKinesisCreds)
    assert(dstream.dynamoDBCreds == Option(customDynamoDBCreds))
    assert(dstream.cloudWatchCreds == Option(customCloudWatchCreds))
    assert(dstream.metricsLevel == customMetricsLevel)
    assert(dstream.metricsEnabledDimensions == customMetricsEnabledDimensions)

    // Testing with AtTimestamp
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val timestamp = cal.getTime()
    val initialPositionAtTimestamp = new AtTimestamp(timestamp)

    val dstreamAtTimestamp = builder
      .endpointUrl(customEndpointUrl)
      .regionName(customRegion)
      .initialPosition(initialPositionAtTimestamp)
      .checkpointAppName(customAppName)
      .checkpointInterval(customCheckpointInterval)
      .storageLevel(customStorageLevel)
      .kinesisCredentials(customKinesisCreds)
      .dynamoDBCredentials(customDynamoDBCreds)
      .cloudWatchCredentials(customCloudWatchCreds)
      .metricsLevel(customMetricsLevel)
      .metricsEnabledDimensions(customMetricsEnabledDimensions)
      .build()
    assert(dstreamAtTimestamp.endpointUrl == customEndpointUrl)
    assert(dstreamAtTimestamp.regionName == customRegion)
    assert(dstreamAtTimestamp.initialPosition.getPosition
      == initialPositionAtTimestamp.getPosition)
    assert(
      dstreamAtTimestamp.initialPosition.asInstanceOf[AtTimestamp].getTimestamp.equals(timestamp))
    assert(dstreamAtTimestamp.checkpointAppName == customAppName)
    assert(dstreamAtTimestamp.checkpointInterval == customCheckpointInterval)
    assert(dstreamAtTimestamp._storageLevel == customStorageLevel)
    assert(dstreamAtTimestamp.kinesisCreds == customKinesisCreds)
    assert(dstreamAtTimestamp.dynamoDBCreds == Option(customDynamoDBCreds))
    assert(dstreamAtTimestamp.cloudWatchCreds == Option(customCloudWatchCreds))
    assert(dstreamAtTimestamp.metricsLevel == customMetricsLevel)
    assert(dstreamAtTimestamp.metricsEnabledDimensions == customMetricsEnabledDimensions)
  }

  test("old Api should throw UnsupportedOperationExceptionexception with AT_TIMESTAMP") {
    val streamName: String = "a-very-nice-stream-name"
    val endpointUrl: String = "https://kinesis.us-west-2.amazonaws.com"
    val region: String = "us-west-2"
    val appName: String = "a-very-nice-kinesis-app"
    val checkpointInterval: Duration = Seconds.apply(30)
    val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY

    // This should not build.
    // InitialPositionInStream.AT_TIMESTAMP is not supported in old Api.
    // The builder Api in KinesisInputDStream should be used.
    intercept[UnsupportedOperationException] {
      val kinesisDStream: KinesisInputDStream[Array[Byte]] = KinesisInputDStream.builder
        .streamingContext(ssc)
        .streamName(streamName)
        .endpointUrl(endpointUrl)
        .regionName(region)
        .initialPositionInStream(InitialPositionInStream.AT_TIMESTAMP)
        .checkpointAppName(appName)
        .checkpointInterval(checkpointInterval)
        .storageLevel(storageLevel)
        .build()
    }
  }
}
