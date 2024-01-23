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

import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Random, Success, Try}

import software.amazon.awssdk.auth.credentials.{AwsCredentials, DefaultCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.servicemetadata.KinesisServiceMetadata
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.{CreateStreamRequest, DeleteStreamRequest, DescribeStreamRequest, MergeShardsRequest, PutRecordRequest, ResourceNotFoundException, Shard, SplitShardRequest, StreamDescription}

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{STREAM_NAME, TABLE_NAME}

/**
 * Shared utility methods for performing Kinesis tests that actually transfer data.
 *
 * PLEASE KEEP THIS FILE UNDER src/main AS PYTHON TESTS NEED ACCESS TO THIS FILE!
 */
private[kinesis] class KinesisTestUtils(streamShardCount: Int = 2) extends Logging {

  val endpointUrl = KinesisTestUtils.endpointUrl
  val regionName = KinesisTestUtils.getRegionNameByEndpoint(endpointUrl)

  private val createStreamTimeoutSeconds = 300
  private val describeStreamPollTimeSeconds = 1

  @volatile
  private var streamCreated = false

  @volatile
  private var _streamName: String = _

  protected lazy val kinesisClient: KinesisClient = {
    KinesisClient.builder()
      .credentialsProvider(DefaultCredentialsProvider.create())
      .region(Region.of(regionName))
      .httpClientBuilder(ApacheHttpClient.builder())
      .endpointOverride(URI.create(endpointUrl))
      .build()
  }

  private lazy val dynamoDB = {
    DynamoDbClient.builder()
      .credentialsProvider(DefaultCredentialsProvider.create())
      .region(Region.of(regionName))
      .httpClientBuilder(ApacheHttpClient.builder())
      .build()
  }

  protected def getProducer(aggregate: Boolean): KinesisDataGenerator = {
    if (!aggregate) {
      new SimpleDataGenerator(kinesisClient)
    } else {
      throw new UnsupportedOperationException("Aggregation is not supported through this code path")
    }
  }

  def streamName: String = {
    require(streamCreated, "Stream not yet created, call createStream() to create one")
    _streamName
  }

  def createStream(): Unit = {
    require(!streamCreated, "Stream already created")
    _streamName = findNonExistentStreamName()

    // Create a stream. The number of shards determines the provisioned throughput.
    logInfo(s"Creating stream ${_streamName}")
    val createStreamRequest = CreateStreamRequest.builder()
      .streamName(_streamName)
      .shardCount(streamShardCount)
      .build()
    kinesisClient.createStream(createStreamRequest)

    // The stream is now being created. Wait for it to become active.
    waitForStreamToBeActive(_streamName)
    streamCreated = true
    logInfo(s"Created stream ${_streamName}")
  }

  def getShards: Seq[Shard] = {
    val describeStreamRequest = DescribeStreamRequest.builder()
      .streamName(_streamName)
      .build()
    kinesisClient.describeStream(describeStreamRequest).streamDescription.shards.asScala.toSeq
  }

  def splitShard(shardId: String): Unit = {
    val splitShardRequest = SplitShardRequest.builder()
      .streamName(_streamName)
      .shardToSplit(shardId)
      // Set a half of the max hash value
      .newStartingHashKey("170141183460469231731687303715884105728")
      .build()
    kinesisClient.splitShard(splitShardRequest)
    // Wait for the shards to become active
    waitForStreamToBeActive(_streamName)
  }

  def mergeShard(shardToMerge: String, adjacentShardToMerge: String): Unit = {
    val mergeShardRequest = MergeShardsRequest.builder()
      .streamName(_streamName)
      .shardToMerge(shardToMerge)
      .adjacentShardToMerge(adjacentShardToMerge)
      .build()
    kinesisClient.mergeShards(mergeShardRequest)
    // Wait for the shards to become active
    waitForStreamToBeActive(_streamName)
  }

  /**
   * Push data to Kinesis stream and return a map of
   * shardId -> seq of (data, seq number) pushed to corresponding shard
   */
  def pushData(testData: Seq[Int], aggregate: Boolean): Map[String, Seq[(Int, String)]] = {
    require(streamCreated, "Stream not yet created, call createStream() to create one")
    val producer = getProducer(aggregate)
    val shardIdToSeqNumbers = producer.sendData(streamName, testData)
    logInfo(s"Pushed $testData:\n\t ${shardIdToSeqNumbers.mkString("\n\t")}")
    shardIdToSeqNumbers
  }

  /**
   * Expose a Python friendly API.
   */
  def pushData(testData: java.util.List[Int]): Unit = {
    pushData(testData.asScala.toSeq, aggregate = false)
  }

  def deleteStream(): Unit = {
    val deleteStreamRequest = DeleteStreamRequest.builder()
      .streamName(streamName)
      .build()
    try {
      if (streamCreated) {
        kinesisClient.deleteStream(deleteStreamRequest)
      }
    } catch {
      case e: Exception =>
        logWarning(log"Could not delete stream ${MDC(STREAM_NAME, streamName)}", e)
    }
  }

  def deleteDynamoDBTable(tableName: String): Unit = {
    val deleteTableRequest = DeleteTableRequest.builder()
      .tableName(tableName)
      .build()
    try {
      dynamoDB.deleteTable(deleteTableRequest)
    } catch {
      case e: Exception =>
        logWarning(log"Could not delete DynamoDB table ${MDC(TABLE_NAME, tableName)}", e)
    }
  }

  private def describeStream(streamNameToDescribe: String): Option[StreamDescription] = {
    try {
      val describeStreamRequest = DescribeStreamRequest.builder()
        .streamName(streamNameToDescribe)
        .build()
      val desc = kinesisClient.describeStream(describeStreamRequest).streamDescription
      Some(desc)
    } catch {
      case rnfe: ResourceNotFoundException =>
        logWarning(s"Could not describe stream $streamNameToDescribe", rnfe)
        None
    }
  }

  private def findNonExistentStreamName(): String = {
    var testStreamName: String = null
    do {
      Thread.sleep(TimeUnit.SECONDS.toMillis(describeStreamPollTimeSeconds))
      testStreamName = s"KinesisTestUtils-${math.abs(Random.nextLong())}"
    } while (describeStream(testStreamName).nonEmpty)
    testStreamName
  }

  private def waitForStreamToBeActive(streamNameToWaitFor: String): Unit = {
    val startTimeNs = System.nanoTime()
    while (System.nanoTime() - startTimeNs < TimeUnit.SECONDS.toNanos(createStreamTimeoutSeconds)) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(describeStreamPollTimeSeconds))
      describeStream(streamNameToWaitFor).foreach { description =>
        val streamStatus = description.streamStatus
        logDebug(s"\t- current state: $streamStatus\n")
        if ("ACTIVE".equals(streamStatus.toString)) {
          return
        }
      }
    }
    require(false, s"Stream $streamName never became active")
  }
}

private[kinesis] object KinesisTestUtils {

  val envVarNameForEnablingTests = "ENABLE_KINESIS_TESTS"
  val endVarNameForEndpoint = "KINESIS_TEST_ENDPOINT_URL"
  val defaultEndpointUrl = "https://kinesis.us-west-2.amazonaws.com"

  def getRegionNameByEndpoint(endpoint: String): String = {
    val uri = new java.net.URI(endpoint)
    val kinesisServiceMetadata = new KinesisServiceMetadata()
    kinesisServiceMetadata.regions
      .asScala
      .find(r => kinesisServiceMetadata.endpointFor(r).toString.equals(uri.getHost))
      .map(_.id)
      .getOrElse(
        throw new IllegalArgumentException(s"Could not resolve region for endpoint: $endpoint"))
  }

  lazy val shouldRunTests = {
    val isEnvSet = sys.env.get(envVarNameForEnablingTests) == Some("1")
    if (isEnvSet) {
      // scalastyle:off println
      // Print this so that they are easily visible on the console and not hidden in the log4j logs.
      println(
        s"""
          |Kinesis tests that actually send data has been enabled by setting the environment
          |variable $envVarNameForEnablingTests to 1. This will create Kinesis Streams and
          |DynamoDB tables in AWS. Please be aware that this may incur some AWS costs.
          |By default, the tests use the endpoint URL $defaultEndpointUrl to create Kinesis streams.
          |To change this endpoint URL to a different region, you can set the environment variable
          |$endVarNameForEndpoint to the desired endpoint URL
          |(e.g. $endVarNameForEndpoint="https://kinesis.us-west-2.amazonaws.com").
        """.stripMargin)
      // scalastyle:on println
    }
    isEnvSet
  }

  lazy val endpointUrl = {
    val url = sys.env.getOrElse(endVarNameForEndpoint, defaultEndpointUrl)
    // scalastyle:off println
    // Print this so that they are easily visible on the console and not hidden in the log4j logs.
    println(s"Using endpoint URL $url for creating Kinesis streams for tests.")
    // scalastyle:on println
    url
  }

  def isAWSCredentialsPresent: Boolean = {
    Try { DefaultCredentialsProvider.create().resolveCredentials() }.isSuccess
  }

  def getAWSCredentials(): AwsCredentials = {
    assert(shouldRunTests,
      "Kinesis test not enabled, should not attempt to get AWS credentials")
    Try { DefaultCredentialsProvider.create().resolveCredentials() } match {
      case Success(cred) => cred
      case Failure(e) =>
        throw new Exception(
          s"""
             |Kinesis tests enabled using environment variable $envVarNameForEnablingTests
             |but could not find AWS credentials. Please follow instructions in AWS documentation
             |to set the credentials in your system such that the DefaultCredentialsProvider
             |can find the credentials.
           """.stripMargin)
    }
  }

  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    val endpointUrl = "https://kinesis.us-west-2.amazonaws.com"
    println(s"Result: ${getRegionNameByEndpoint(endpointUrl)}")
    // scalastyle:on println
  }
}

/** A wrapper interface that will allow us to consolidate the code for synthetic data generation. */
private[kinesis] trait KinesisDataGenerator {
  /** Sends the data to Kinesis and returns the metadata for everything that has been sent. */
  def sendData(streamName: String, data: Seq[Int]): Map[String, Seq[(Int, String)]]
}

private[kinesis] class SimpleDataGenerator(
    client: KinesisClient) extends KinesisDataGenerator {
  override def sendData(streamName: String, data: Seq[Int]): Map[String, Seq[(Int, String)]] = {
    val shardIdToSeqNumbers = new mutable.HashMap[String, ArrayBuffer[(Int, String)]]()
    data.foreach { num =>
      val str = num.toString
      val data = ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8))
      val putRecordRequest = PutRecordRequest.builder()
        .streamName(streamName)
        .data(SdkBytes.fromByteBuffer(data))
        .partitionKey(str)
        .build()

      val putRecordResponse = client.putRecord(putRecordRequest)
      val shardId = putRecordResponse.shardId
      val seqNumber = putRecordResponse.sequenceNumber
      val sentSeqNumbers = shardIdToSeqNumbers.getOrElseUpdate(shardId,
        new ArrayBuffer[(Int, String)]())
      sentSeqNumbers += ((num, seqNumber))
    }

    shardIdToSeqNumbers.toMap.transform((_, v) => v.toSeq)
  }
}
