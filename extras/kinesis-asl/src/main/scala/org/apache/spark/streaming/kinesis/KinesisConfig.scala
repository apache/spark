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

import scala.reflect.ClassTag


import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.{RegionUtils, Region}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient


case class KinesisConfig(
    kinesisAppName: String,
    streamName: String,
    endpointUrl: String,
    regionName: String,
    initialPositionInStream: InitialPositionInStream = InitialPositionInStream.TRIM_HORIZON,
    awsCredentialsOption: Option[SerializableAWSCredentials] = None,
    dynamoEndpointUrl: Option[String] = None,
    dynamoCredentials: Option[SerializableAWSCredentials] = None
    ) {

  def buildKCLConfig(workerId: String): KinesisClientLibConfiguration = {
    // KCL config instance
    val kinesisClientLibConfiguration =
      new KinesisClientLibConfiguration(kinesisAppName, streamName, resolveAWSCredentialsProvider(), workerId)
      .withKinesisEndpoint(endpointUrl)
      .withInitialPositionInStream(initialPositionInStream)
      .withTaskBackoffTimeMillis(500)
      .withRegionName(regionName)
    return kinesisClientLibConfiguration

  }

  def region: Region = {
    RegionUtils.getRegion(regionName)
  }

  def buildDynamoClient(): AmazonDynamoDBClient = {
    val client = if (dynamoCredentials.isDefined) new AmazonDynamoDBClient(resolveAWSCredentialsProvider(dynamoCredentials)) else new AmazonDynamoDBClient(resolveAWSCredentialsProvider())
    client.setRegion(region)
    if (dynamoEndpointUrl.isDefined) {
      client.setEndpoint(dynamoEndpointUrl.get)
    }
    client
  }

  def buildKinesisClient(): AmazonKinesisClient = {
    val client = new AmazonKinesisClient(resolveAWSCredentialsProvider())
    client.setRegion(region)
    client.setEndpoint(endpointUrl)
    client

  }

  def buildCloudwatchClient(): AmazonCloudWatchClient = {
    val client = new AmazonCloudWatchClient(resolveAWSCredentialsProvider())
    client.setRegion(region)
    client

  }

  def awsCreds: AWSCredentials = {
    awsCredentialsOption.getOrElse(new DefaultAWSCredentialsProviderChain().getCredentials())

  }


  /**
   * If AWS credential is provided, return a AWSCredentialProvider returning that credential.
   * Otherwise, return the DefaultAWSCredentialsProviderChain.
   */
  private def resolveAWSCredentialsProvider(awsCredOpt: Option[SerializableAWSCredentials] = awsCredentialsOption): AWSCredentialsProvider = {
    awsCredOpt match {
      case Some(awsCredentials) =>
        new AWSCredentialsProvider {
          override def getCredentials: AWSCredentials = awsCredentials
          override def refresh(): Unit = { }
        }
      case None =>
        new DefaultAWSCredentialsProviderChain()
    }
  }

}

case class SerializableAWSCredentials(accessKeyId: String, secretKey: String)
  extends AWSCredentials {
  override def getAWSAccessKeyId: String = accessKeyId
  override def getAWSSecretKey: String = secretKey
}

private object KinesisConfig {


  /*
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   *
   */
  def buildConfig(
    kinesisAppName: String,
    streamName: String,
    endpointUrl: String,
    regionName: String,
    initialPositionInStream: InitialPositionInStream = InitialPositionInStream.TRIM_HORIZON,
    awsCredentialsOption: Option[SerializableAWSCredentials] = None): KinesisConfig = {
    new KinesisConfig(kinesisAppName, streamName, endpointUrl,
        regionName, initialPositionInStream, awsCredentialsOption)
  }

}