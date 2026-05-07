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

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentialsProvider, DefaultCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import org.apache.spark.internal.Logging

/**
 * Serializable interface providing a method executors can call to obtain an
 * AwsCredentialsProvider instance for authenticating to AWS services.
 */
private[kinesis] sealed trait SparkAWSCredentials extends Serializable {
  /**
   * Return an AwsCredentialProvider instance that can be used by the Kinesis Client
   * Library to authenticate to AWS services (Kinesis, CloudWatch and DynamoDB).
   */
  def provider: AwsCredentialsProvider
}

/** Returns DefaultCredentialsProvider for authentication. */
private[kinesis] final case object DefaultCredentials extends SparkAWSCredentials {

  def provider: AwsCredentialsProvider = DefaultCredentialsProvider.create()
}

/**
 * Returns StaticCredentialsProvider constructed using basic AWS keypair. Falls back to using
 * DefaultCredentialsProvider if unable to construct a StaticCredentialsProvider
 * instance with the provided arguments (e.g. if they are null).
 */
private[kinesis] final case class BasicCredentials(
    awsAccessKeyId: String,
    awsSecretKey: String) extends SparkAWSCredentials with Logging {

  def provider: AwsCredentialsProvider = try {
    StaticCredentialsProvider.create(AwsBasicCredentials.create(awsAccessKeyId, awsSecretKey))
  } catch {
    case e: IllegalArgumentException =>
      logWarning("Unable to construct StaticCredentialsProvider with provided keypair; " +
        "falling back to DefaultCredentialsProvider.", e)
      DefaultCredentialsProvider.create()
  }
}

/**
 * Returns an StsAssumeRoleCredentialsProvider instance which assumes an IAM
 * role in order to authenticate against resources in an external account.
 */
private[kinesis] final case class STSCredentials(
    stsRoleArn: String,
    stsSessionName: String,
    stsExternalId: Option[String] = None,
    longLivedCreds: SparkAWSCredentials = DefaultCredentials)
  extends SparkAWSCredentials  {

  def provider: AwsCredentialsProvider = {
    val stsClient = StsClient.builder()
      .credentialsProvider(longLivedCreds.provider)
      .build()

    val assumeRoleRequestBuilder = AssumeRoleRequest.builder()
      .roleArn(stsRoleArn)
      .roleSessionName(stsSessionName)
    stsExternalId match {
      case Some(stsExternalId) =>
        assumeRoleRequestBuilder.externalId(stsExternalId)
      case None =>
    }

    StsAssumeRoleCredentialsProvider.builder()
      .stsClient(stsClient)
      .refreshRequest(assumeRoleRequestBuilder.build())
      .build()
  }
}

object SparkAWSCredentials {
  /**
   * Builder for [[SparkAWSCredentials]] instances.
   *
   * @since 2.2.0
   */
  class Builder {
    private var basicCreds: Option[BasicCredentials] = None
    private var stsCreds: Option[STSCredentials] = None

    // scalastyle:off
    /**
     * Use a basic AWS keypair for long-lived authorization.
     *
     * @note The given AWS keypair will be saved in DStream checkpoints if checkpointing is
     * enabled. Make sure that your checkpoint directory is secure. Prefer using the
     * default credentials provider instead if possible
     * (https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html).
     *
     * @param accessKeyId AWS access key ID
     * @param secretKey AWS secret key
     * @return Reference to this [[SparkAWSCredentials.Builder]]
     */
    // scalastyle:on
    def basicCredentials(accessKeyId: String, secretKey: String): Builder = {
      basicCreds = Option(BasicCredentials(
        awsAccessKeyId = accessKeyId,
        awsSecretKey = secretKey))
      this
    }

    /**
     * Use STS to assume an IAM role for temporary session-based authentication. Will use configured
     * long-lived credentials for authorizing to STS itself (either the default provider chain
     * or a configured keypair).
     *
     * @param roleArn ARN of IAM role to assume via STS
     * @param sessionName Name to use for the STS session
     * @return Reference to this [[SparkAWSCredentials.Builder]]
     */
    def stsCredentials(roleArn: String, sessionName: String): Builder = {
      stsCreds = Option(STSCredentials(stsRoleArn = roleArn, stsSessionName = sessionName))
      this
    }

    /**
     * Use STS to assume an IAM role for temporary session-based authentication. Will use configured
     * long-lived credentials for authorizing to STS itself (either the default provider chain
     * or a configured keypair). STS will validate the provided external ID with the one defined
     * in the trust policy of the IAM role to be assumed (if one is present).
     *
     * @param roleArn ARN of IAM role to assume via STS
     * @param sessionName Name to use for the STS session
     * @param externalId External ID to validate against assumed IAM role's trust policy
     * @return Reference to this [[SparkAWSCredentials.Builder]]
     */
    def stsCredentials(roleArn: String, sessionName: String, externalId: String): Builder = {
      stsCreds = Option(STSCredentials(
        stsRoleArn = roleArn,
        stsSessionName = sessionName,
        stsExternalId = Option(externalId)))
      this
    }

    /**
     * Returns the appropriate instance of [[SparkAWSCredentials]] given the configured
     * parameters.
     *
     * - The long-lived credentials will either be [[DefaultCredentials]] or [[BasicCredentials]]
     *   if they were provided.
     *
     * - If STS credentials were provided, the configured long-lived credentials will be added to
     *   them and the result will be returned.
     *
     * - The long-lived credentials will be returned otherwise.
     *
     * @return [[SparkAWSCredentials]] to use for configured parameters
     */
    def build(): SparkAWSCredentials =
      stsCreds.map(_.copy(longLivedCreds = longLivedCreds)).getOrElse(longLivedCreds)

    private def longLivedCreds: SparkAWSCredentials = basicCreds.getOrElse(DefaultCredentials)
  }

  /**
   * Creates a [[SparkAWSCredentials.Builder]] for constructing
   * [[SparkAWSCredentials]] instances.
   *
   * @since 2.2.0
   *
   * @return [[SparkAWSCredentials.Builder]] instance
   */
  def builder: Builder = new Builder
}
