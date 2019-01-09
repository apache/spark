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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Random, Success, Try}

import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}
import com.amazonaws.services.kinesis.model._

import org.apache.spark.internal.Logging

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

  protected lazy val kinesisClient = {
    val client = new AmazonKinesisClient(KinesisTestUtils.getAWSCredentials())
    client.setEndpoint(endpointUrl)
    client
  }

  private lazy val dynamoDB = {
    val dynamoDBClient = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain())
    dynamoDBClient.setRegion(RegionUtils.getRegion(regionName))
    new DynamoDB(dynamoDBClient)
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
    val createStreamRequest = new CreateStreamRequest()
    createStreamRequest.setStreamName(_streamName)
    createStreamRequest.setShardCount(streamShardCount)
    kinesisClient.createStream(createStreamRequest)

    // The stream is now being created. Wait for it to become active.
    waitForStreamToBeActive(_streamName)
    streamCreated = true
    logInfo(s"Created stream ${_streamName}")
  }

  def getShards(): Seq[Shard] = {
    kinesisClient.describeStream(_streamName).getStreamDescription.getShards.asScala
  }

  def splitShard(shardId: String): Unit = {
    val splitShardRequest = new SplitShardRequest()
    splitShardRequest.withStreamName(_streamName)
    splitShardRequest.withShardToSplit(shardId)
    // Set a half of the max hash value
    splitShardRequest.withNewStartingHashKey("170141183460469231731687303715884105728")
    kinesisClient.splitShard(splitShardRequest)
    // Wait for the shards to become active
    waitForStreamToBeActive(_streamName)
  }

  def mergeShard(shardToMerge: String, adjacentShardToMerge: String): Unit = {
    val mergeShardRequest = new MergeShardsRequest
    mergeShardRequest.withStreamName(_streamName)
    mergeShardRequest.withShardToMerge(shardToMerge)
    mergeShardRequest.withAdjacentShardToMerge(adjacentShardToMerge)
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
    shardIdToSeqNumbers.toMap
  }

  /**
   * Expose a Python friendly API.
   */
  def pushData(testData: java.util.List[Int]): Unit = {
    pushData(testData.asScala, aggregate = false)
  }

  def deleteStream(): Unit = {
    try {
      if (streamCreated) {
        kinesisClient.deleteStream(streamName)
      }
    } catch {
      case e: Exception =>
        logWarning(s"Could not delete stream $streamName")
    }
  }

  def deleteDynamoDBTable(tableName: String): Unit = {
    try {
      val table = dynamoDB.getTable(tableName)
      table.delete()
      table.waitForDelete()
    } catch {
      case e: Exception =>
        logWarning(s"Could not delete DynamoDB table $tableName")
    }
  }

  private def describeStream(streamNameToDescribe: String): Option[StreamDescription] = {
    try {
      val describeStreamRequest = new DescribeStreamRequest().withStreamName(streamNameToDescribe)
      val desc = kinesisClient.describeStream(describeStreamRequest).getStreamDescription()
      Some(desc)
    } catch {
      case rnfe: ResourceNotFoundException =>
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
    val startTime = System.currentTimeMillis()
    val endTime = startTime + TimeUnit.SECONDS.toMillis(createStreamTimeoutSeconds)
    while (System.currentTimeMillis() < endTime) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(describeStreamPollTimeSeconds))
      describeStream(streamNameToWaitFor).foreach { description =>
        val streamStatus = description.getStreamStatus()
        logDebug(s"\t- current state: $streamStatus\n")
        if ("ACTIVE".equals(streamStatus)) {
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
    RegionUtils.getRegionsForService(AmazonKinesis.ENDPOINT_PREFIX)
      .asScala
      .find(_.getAvailableEndpoints.asScala.toSeq.contains(uri.getHost))
      .map(_.getName)
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
    Try { new DefaultAWSCredentialsProviderChain().getCredentials() }.isSuccess
  }

  def getAWSCredentials(): AWSCredentials = {
    assert(shouldRunTests,
      "Kinesis test not enabled, should not attempt to get AWS credentials")
    Try { new DefaultAWSCredentialsProviderChain().getCredentials() } match {
      case Success(cred) => cred
      case Failure(e) =>
        throw new Exception(
          s"""
             |Kinesis tests enabled using environment variable $envVarNameForEnablingTests
             |but could not find AWS credentials. Please follow instructions in AWS documentation
             |to set the credentials in your system such that the DefaultAWSCredentialsProviderChain
             |can find the credentials.
           """.stripMargin)
    }
  }
}

/** A wrapper interface that will allow us to consolidate the code for synthetic data generation. */
private[kinesis] trait KinesisDataGenerator {
  /** Sends the data to Kinesis and returns the metadata for everything that has been sent. */
  def sendData(streamName: String, data: Seq[Int]): Map[String, Seq[(Int, String)]]
}

private[kinesis] class SimpleDataGenerator(
    client: AmazonKinesisClient) extends KinesisDataGenerator {
  override def sendData(streamName: String, data: Seq[Int]): Map[String, Seq[(Int, String)]] = {
    val shardIdToSeqNumbers = new mutable.HashMap[String, ArrayBuffer[(Int, String)]]()
    data.foreach { num =>
      val str = num.toString
      val data = ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8))
      val putRecordRequest = new PutRecordRequest().withStreamName(streamName)
        .withData(data)
        .withPartitionKey(str)

      val putRecordResult = client.putRecord(putRecordRequest)
      val shardId = putRecordResult.getShardId
      val seqNumber = putRecordResult.getSequenceNumber()
      val sentSeqNumbers = shardIdToSeqNumbers.getOrElseUpdate(shardId,
        new ArrayBuffer[(Int, String)]())
      sentSeqNumbers += ((num, seqNumber))
    }

    shardIdToSeqNumbers.toMap
  }
}
