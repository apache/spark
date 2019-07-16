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

import com.amazonaws.auth._

import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging

/**
 * Serializable interface providing a method executors can call to obtain an
 * AWSCredentialsProvider instance for authenticating to AWS services.
 */
private[kinesis] sealed trait SparkAWSCredentials extends Serializable {
  /**
   * Return an AWSCredentialProvider instance that can be used by the Kinesis Client
   * Library to authenticate to AWS services (Kinesis, CloudWatch and DynamoDB).
   */
  def provider: AWSCredentialsProvider
}

/** Returns DefaultAWSCredentialsProviderChain for authentication. */
private[kinesis] final case object DefaultCredentials extends SparkAWSCredentials {

  def provider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain
}

/**
 * Returns AWSStaticCredentialsProvider constructed using basic AWS keypair. Falls back to using
 * DefaultCredentialsProviderChain if unable to construct a AWSCredentialsProviderChain
 * instance with the provided arguments (e.g. if they are null).
 */
private[kinesis] final case class BasicCredentials(
    awsAccessKeyId: String,
    awsSecretKey: String) extends SparkAWSCredentials with Logging {

  def provider: AWSCredentialsProvider = try {
    new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKeyId, awsSecretKey))
  } catch {
    case e: IllegalArgumentException =>
      logWarning("Unable to construct AWSStaticCredentialsProvider with provided keypair; " +
        "falling back to DefaultCredentialsProviderChain.", e)
      new DefaultAWSCredentialsProviderChain
  }
}

/**
 * Returns an STSAssumeRoleSessionCredentialsProvider instance which assumes an IAM
 * role in order to authenticate against resources in an external account.
 */
private[kinesis] final case class STSCredentials(
    stsRoleArn: String,
    stsSessionName: String,
    stsExternalId: Option[String] = None,
    longLivedCreds: SparkAWSCredentials = DefaultCredentials)
  extends SparkAWSCredentials  {

  def provider: AWSCredentialsProvider = {
    val builder = new STSAssumeRoleSessionCredentialsProvider.Builder(stsRoleArn, stsSessionName)
      .withLongLivedCredentialsProvider(longLivedCreds.provider)
    stsExternalId match {
      case Some(stsExternalId) =>
        builder.withExternalId(stsExternalId)
          .build()
      case None =>
        builder.build()
    }
  }
}

@Evolving
object SparkAWSCredentials {
  /**
   * Builder for [[SparkAWSCredentials]] instances.
   *
   * @since 2.2.0
   */
  @Evolving
  class Builder {
    private var basicCreds: Option[BasicCredentials] = None
    private var stsCreds: Option[STSCredentials] = None

    // scalastyle:off
    /**
     * Use a basic AWS keypair for long-lived authorization.
     *
     * @note The given AWS keypair will be saved in DStream checkpoints if checkpointing is
     * enabled. Make sure that your checkpoint directory is secure. Prefer using the
     * default provider chain instead if possible
     * (http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default).
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
