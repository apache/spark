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

import scala.collection.JavaConverters._

import com.amazonaws.auth._

import org.apache.spark.internal.Logging

/**
 * Serializable interface providing a method executors can call to obtain an
 * AWSCredentialsProvider instance for authenticating to AWS services.
 */
private[kinesis] sealed trait SerializableCredentialsProvider extends Serializable {
  /**
   * Return an AWSCredentialProvider instance that can be used by the Kinesis Client
   * Library to authenticate to AWS services (Kinesis, CloudWatch and DynamoDB).
   */
  def provider: AWSCredentialsProvider
}

/** Returns DefaultAWSCredentialsProviderChain for authentication. */
private[kinesis] final case object DefaultCredentialsProvider
  extends SerializableCredentialsProvider {

  def provider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain
}

/**
 * Returns AWSStaticCredentialsProvider constructed using basic AWS keypair. Falls back to using
 * DefaultAWSCredentialsProviderChain if unable to construct a AWSCredentialsProviderChain
 * instance with the provided arguments (e.g. if they are null).
 */
private[kinesis] final case class BasicCredentialsProvider(
    awsAccessKeyId: String,
    awsSecretKey: String) extends SerializableCredentialsProvider with Logging {

  def provider: AWSCredentialsProvider = try {
    new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKeyId, awsSecretKey))
  } catch {
    case e: IllegalArgumentException =>
      logWarning("Unable to construct AWSStaticCredentialsProvider with provided keypair; " +
        "falling back to DefaultAWSCredentialsProviderChain.", e)
      new DefaultAWSCredentialsProviderChain
  }
}

/**
 * Returns an STSAssumeRoleSessionCredentialsProvider instance which assumes an IAM
 * role in order to authenticate against resources in an external account.
 */
private[kinesis] final case class STSCredentialsProvider(
    stsRoleArn: String,
    stsSessionName: String,
    stsExternalId: Option[String] = None,
    longLivedCredsProvider: SerializableCredentialsProvider = DefaultCredentialsProvider)
  extends SerializableCredentialsProvider  {

  def provider: AWSCredentialsProvider = {
    val builder = new STSAssumeRoleSessionCredentialsProvider.Builder(stsRoleArn, stsSessionName)
      .withLongLivedCredentialsProvider(longLivedCredsProvider.provider)
    stsExternalId match {
      case Some(stsExternalId) =>
        builder.withExternalId(stsExternalId)
          .build()
      case None =>
        builder.build()
    }
  }
}
