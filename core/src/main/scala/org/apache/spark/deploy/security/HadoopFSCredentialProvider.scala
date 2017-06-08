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

package org.apache.spark.deploy.security

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

private[deploy] class HadoopFSCredentialProvider
    extends ServiceCredentialProvider with Logging {
  // Token renewal interval, this value will be set in the first call,
  // if None means no token renewer specified or no token can be renewed,
  // so cannot get token renewal interval.
  private var tokenRenewalInterval: Option[Long] = null

  override val serviceName: String = "hadoopfs"

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    // NameNode to access, used to get tokens from different FileSystems
    val tmpCreds = new Credentials()
    val tokenRenewer = getTokenRenewer(hadoopConf)
    hadoopFSsToAccess(hadoopConf, sparkConf).foreach { dst =>
      val dstFs = dst.getFileSystem(hadoopConf)
      logInfo("getting token for: " + dst)
      dstFs.addDelegationTokens(tokenRenewer, tmpCreds)
    }

    // Get the token renewal interval if it is not set. It will only be called once.
    if (tokenRenewalInterval == null) {
      tokenRenewalInterval = getTokenRenewalInterval(hadoopConf, sparkConf)
    }

    // Get the time of next renewal.
    val nextRenewalDate = tokenRenewalInterval.flatMap { interval =>
      val nextRenewalDates = tmpCreds.getAllTokens.asScala
        .filter(_.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier])
        .map { t =>
          val identifier = t.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
          identifier.getIssueDate + interval
        }
      if (nextRenewalDates.isEmpty) None else Some(nextRenewalDates.min)
    }

    creds.addAll(tmpCreds)
    nextRenewalDate
  }

  protected def getTokenRenewalInterval(
    hadoopConf: Configuration,
    sparkConf: SparkConf): Option[Long] = None

  protected def getTokenRenewer(hadoopConf: Configuration): String = {
    UserGroupInformation.getCurrentUser.getShortUserName
  }

  protected def hadoopFSsToAccess(hadoopConf: Configuration, sparkConf: SparkConf): Set[Path] = {
    Set(FileSystem.get(hadoopConf).getHomeDirectory)
  }
}
