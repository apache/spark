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
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.security.HadoopDelegationTokenProvider

private[deploy] class HadoopFSDelegationTokenProvider
    extends HadoopDelegationTokenProvider with Logging {

  // This tokenRenewalIntervals will be set in the first call to obtainDelegationTokens.
  // If None, no token renewer is specified or no token can be renewed,
  // so we cannot get the token renewal interval.
  private var tokenRenewalIntervals: Map[Text, Long] = null

  override val serviceName: String = "hadoopfs"

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    try {
      val fileSystems = HadoopFSDelegationTokenProvider.hadoopFSsToAccess(sparkConf, hadoopConf)
      val fetchCreds = fetchDelegationTokens(getTokenRenewer(hadoopConf), fileSystems, creds)

      // Get the token renewals interval if it is not set. It will only be called once.
      if (tokenRenewalIntervals == null) {
        tokenRenewalIntervals = getTokenRenewalIntervals(hadoopConf, sparkConf, fileSystems)
      }

      // Construct the map for "token kind" to "issue date", so that we can calculate the
      // next renewal date per token when interval is available.
      val tokenAndIssueDates = fetchCreds.getAllTokens.asScala
        .filter(_.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier])
        .map { token =>
          val identifier = token
            .decodeIdentifier()
            .asInstanceOf[AbstractDelegationTokenIdentifier]
          identifier.getKind -> identifier.getIssueDate
        }.toMap

      // Get the time of next renewal.
      val currentTime = System.currentTimeMillis()
      logDebug(s"Calculating next renewal date per token. Current timestamp is $currentTime")
      val nextRenewalDates = tokenRenewalIntervals.flatMap { case (kind, interval) =>
        tokenAndIssueDates.get(kind).map { issueDate =>
          val nextRenewalDateForToken = issueDate + interval
          logDebug(s"Next renewal date is $nextRenewalDateForToken for token ${kind.toString}")
          nextRenewalDateForToken
        }.filterNot(_ < currentTime)
      }

      if (nextRenewalDates.isEmpty) None else Some(nextRenewalDates.min)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get token from service $serviceName", e)
        None
    }
  }

  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled
  }

  private def getTokenRenewer(hadoopConf: Configuration): String = {
    val tokenRenewer = Master.getMasterPrincipal(hadoopConf)
    logDebug("Delegation token renewer is: " + tokenRenewer)

    if (tokenRenewer == null || tokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer."
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }

    tokenRenewer
  }

  private def fetchDelegationTokens(
      renewer: String,
      filesystems: Set[FileSystem],
      creds: Credentials): Credentials = {

    filesystems.foreach { fs =>
      logInfo(s"getting token for: $fs with renewer $renewer")
      fs.addDelegationTokens(renewer, creds)
    }

    creds
  }

  private def getTokenRenewalIntervals(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      filesystems: Set[FileSystem]): Map[Text, Long] = {
    // We cannot use the tokens generated with renewer yarn. Trying to renew
    // those will fail with an access control issue. So create new tokens with the logged in
    // user as renewer.
    val renewer = UserGroupInformation.getCurrentUser().getUserName()

    val creds = new Credentials()
    fetchDelegationTokens(renewer, filesystems, creds)

    creds.getAllTokens.asScala.filter {
      _.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier]
    }.flatMap { token =>
      Try {
        val newExpiration = token.renew(hadoopConf)
        val identifier = token.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
        val interval = newExpiration - identifier.getIssueDate
        logInfo(s"Renewal interval is $interval for token ${token.getKind.toString}")
        (token.getKind -> interval)
      }.toOption
    }.toMap
  }
}

private[deploy] object HadoopFSDelegationTokenProvider {
  def hadoopFSsToAccess(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Set[FileSystem] = {
    // scalastyle:off FileSystemGet
    val defaultFS = FileSystem.get(hadoopConf)
    // scalastyle:on FileSystemGet

    val filesystemsToAccess = sparkConf.get(KERBEROS_FILESYSTEMS_TO_ACCESS)
      .map(new Path(_).getFileSystem(hadoopConf))
      .toSet

    val master = sparkConf.get("spark.master", null)
    val stagingFS = if (master != null && master.contains("yarn")) {
      sparkConf.get(STAGING_DIR).map(new Path(_).getFileSystem(hadoopConf))
    } else {
      None
    }

    filesystemsToAccess ++ stagingFS + defaultFS
  }
}
