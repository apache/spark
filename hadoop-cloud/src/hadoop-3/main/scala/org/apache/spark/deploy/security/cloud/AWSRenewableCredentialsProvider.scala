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

package org.apache.spark.deploy.security.cloud

import scala.util.control.NonFatal

import com.amazonaws.auth.{AWSCredentials, EnvironmentVariableCredentialsProvider, STSSessionCredentialsProvider}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

class AWSRenewableCredentialsProvider
    extends CloudCredentialsProvider with Logging {

  override val serviceName: String = "aws"

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf ): Option[CloudCredentials] = {
    try {

      val newCredentials: CloudCredentials = {
        val stsCredentials = new SessionCredentialsProvider(
          new EnvironmentVariableCredentialsProvider().getCredentials)
        val expiry = System.currentTimeMillis() + stsCredentials.duration // 8 hrs
        CloudCredentials(serviceName, stsCredentials.toString, Some(expiry))
      }
      Some(newCredentials)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get credentials from service $serviceName", e)
        None
    }
  }

  class SessionCredentialsProvider (longLivedCredentials: AWSCredentials) {
    private val credentials = new STSSessionCredentialsProvider(longLivedCredentials).getCredentials

    // validity duration in milliseconds
    val duration: Long = STSSessionCredentialsProvider.DEFAULT_DURATION_SECONDS * 1000

    override def toString: String =
      s"""{
         |accessKeyId: ${credentials.getAWSAccessKeyId},
         |secretAccessKey: ${credentials.getAWSSecretKey},
         |sessionToken: ${credentials.getSessionToken}
         |}""".stripMargin
  }
}
