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
import com.amazonaws.regions.{Region, RegionUtils}
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}


/**
 * Configuration container for settings to be passed down into the kinesis-client-library (KCL).
 * This class is also used to build any of the client instances used by the KCL so we
 * can override the things like the endpoint.
 *
 *
 * @param kinesisAppName The name of kinesis application (used in creating dynamo tables)
 * @param streamName The name of the actual kinesis stream
 * @param endpointUrl The AWS API endpoint that will be used for the kinesis client
 * @param regionName The AWS region that will be connected to
 *                  (will set default enpoint for dynamo and cloudwatch)
 * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
 *                                 worker's initial starting position in the stream.
 *                                 The values are either the beginning of the stream
 *                                 per Kinesis' limit of 24 hours
 *                                 (InitialPositionInStream.TRIM_HORIZON) or
 *                                 the tip of the stream (InitialPositionInStream.LATEST).
 * @param awsCredentialsOption None or Some instance of SerializableAWSCredentials that
 *                             will be used for credentials for Kinesis and the default
 *                             for other clients. If None, then the
 *                             DefaultAWSCredentialsProviderChain will be used
 * @param dynamoEndpointUrl None or Some AWS API endpoint that will be used for
 *                          the DynamoDBClient, if None, then the regionName
 *                          will be used to build the default endpoint
 * @param dynamoCredentials None or Some SerializableAWSCredentials that will be used
 *                          as the credentials. If None, then the
 *                          DefaultProviderKeychain will be used to build credentials
 *
 */
case class KinesisConfig(
    kinesisAppName: String,
    streamName: String,
    endpointUrl: String,
    regionName: String,
    initialPositionInStream: InitialPositionInStream = InitialPositionInStream.TRIM_HORIZON,
    awsCredentialsOption: Option[SerializableAWSCredentials] = None,
    dynamoEndpointUrl: Option[String] = None,
    dynamoCredentials: Option[SerializableAWSCredentials] = None
    ) extends Serializable {

  /**
   * Builds a KinesisClientLibConfiguration object, which contains all the configuration options
   * See the
   * <a href="http://bit.ly/1oyynyW">KinesisClientLibConfiguration docs</a>
   *  for more info:
   *
   *
   * @param workerId A unique string to identify a worker
   */
  def buildKCLConfig(workerId: String): KinesisClientLibConfiguration = {
    // KCL config instance
    val kinesisClientLibConfiguration =
      new KinesisClientLibConfiguration(
          kinesisAppName,
          streamName,
          resolveAWSCredentialsProvider(),
          workerId)
      .withKinesisEndpoint(endpointUrl)
      .withInitialPositionInStream(initialPositionInStream)
      .withTaskBackoffTimeMillis(500)
    return kinesisClientLibConfiguration

  }


  /**
   * Returns a AmazonDynamoDBClient instance configured with the proper region/endpoint
   */
  def buildDynamoClient(): AmazonDynamoDBClient = {
    val client = if (dynamoCredentials.isDefined) {
      new AmazonDynamoDBClient(resolveAWSCredentialsProvider(dynamoCredentials))
    } else {
      new AmazonDynamoDBClient(resolveAWSCredentialsProvider())
    }

    if (dynamoEndpointUrl.isDefined) {
      client.withEndpoint(dynamoEndpointUrl.get)
    } else {
      client.withRegion(region)
    }
  }

  /**
   * Returns a AmazonKinesisClient instance configured with the proper region/endpoint
   */
  def buildKinesisClient(): AmazonKinesisClient = {
    val client = new AmazonKinesisClient(resolveAWSCredentialsProvider())
    client.withEndpoint(endpointUrl)

  }

  /**
   * Returns a AmazonCloudWatchClient instance configured with the proper region/endpoint
   */
  def buildCloudwatchClient(): AmazonCloudWatchClient = {
    val client = new AmazonCloudWatchClient(resolveAWSCredentialsProvider())
    client.withRegion(region)
  }

  /**
   * Returns the provided credentials or resolves a
   * pair of credentials using DefaultAWSCredentialsProviderChain
   */
  def awsCredentials: AWSCredentials = {
    resolveAWSCredentialsProvider().getCredentials()
  }


  /**
   * If AWS credential is provided, return a AWSCredentialProvider returning that credential.
   * Otherwise, return the DefaultAWSCredentialsProviderChain.
   */
  private def resolveAWSCredentialsProvider(
      awsCredOpt: Option[SerializableAWSCredentials] = awsCredentialsOption
      ): AWSCredentialsProvider = {
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

  /**
   * Resolves string region into the region object
   */
  private def region: Region = {
    RegionUtils.getRegion(regionName)
  }

}

/**
 * A small class that extends AWSCredentials that is marked as serializable, which
 * is needed in order to have it serialized into a spark context
 *
 * @param accessKeyId An AWS accessKeyId
 * @param secretKey An AWS secretKey
 */
case class SerializableAWSCredentials(accessKeyId: String, secretKey: String)
  extends AWSCredentials {
  override def getAWSAccessKeyId: String = accessKeyId
  override def getAWSSecretKey: String = secretKey
}

private object KinesisConfig {
  /**
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
