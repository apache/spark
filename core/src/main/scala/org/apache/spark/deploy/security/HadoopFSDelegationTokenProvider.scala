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

import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.apache.spark.util.Utils

private[deploy] class HadoopFSDelegationTokenProvider
    extends HadoopDelegationTokenProvider with Logging {

  // This tokenRenewalInterval will be set in the first call to obtainDelegationTokens.
  // If None, no token renewer is specified or no token can be renewed,
  // so we cannot get the token renewal interval.
  private var tokenRenewalInterval: Option[Long] = null

  override val serviceName: String = "hadoopfs"

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    try {
      val fileSystems = HadoopFSDelegationTokenProvider.hadoopFSsToAccess(sparkConf, hadoopConf)
      // The hosts on which the file systems to be excluded from token renewal
      val fsToExclude = sparkConf.get(YARN_KERBEROS_FILESYSTEM_RENEWAL_EXCLUDE)
        .map(new Path(_).getFileSystem(hadoopConf).getUri.getHost)
        .toSet
      val fetchCreds = fetchDelegationTokens(getTokenRenewer(sparkConf, hadoopConf), fileSystems,
        creds, fsToExclude)

      // Get the token renewal interval if it is not set. It will only be called once.
      if (tokenRenewalInterval == null) {
        tokenRenewalInterval = getTokenRenewalInterval(hadoopConf, fileSystems)
      }

      // Get the time of next renewal.
      val nextRenewalDate = tokenRenewalInterval.flatMap { interval =>
        val nextRenewalDates = fetchCreds.getAllTokens.asScala
          .filter(_.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier])
          .map { token =>
            val identifier = token
              .decodeIdentifier()
              .asInstanceOf[AbstractDelegationTokenIdentifier]
            val tokenKind = token.getKind.toString
            getIssueDate(tokenKind, identifier) + interval
          }
        if (nextRenewalDates.isEmpty) None else Some(nextRenewalDates.min)
      }

      nextRenewalDate
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to get token from service ${MDC(SERVICE_NAME, serviceName)}", e)
        None
    }
  }

  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled
  }

  private def getTokenRenewer(sparkConf: SparkConf, hadoopConf: Configuration): String = {
    val master = sparkConf.get("spark.master", null)
    val tokenRenewer = if (master != null && master.contains("yarn")) {
      Master.getMasterPrincipal(hadoopConf)
    } else {
      UserGroupInformation.getCurrentUser().getUserName()
    }
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
      creds: Credentials,
      fsToExclude: Set[String]): Credentials = {

    filesystems.foreach { fs =>
      if (fsToExclude.contains(fs.getUri.getHost)) {
        // YARN RM skips renewing token with empty renewer
        logInfo(log"getting token for: ${MDC(FILE_SYSTEM, fs)} with empty renewer to skip renewal")
        Utils.tryLogNonFatalError { fs.addDelegationTokens("", creds) }
      } else {
        logInfo(log"getting token for: ${MDC(FILE_SYSTEM, fs)} with" +
          log" renewer ${MDC(TOKEN_RENEWER, renewer)}")
        Utils.tryLogNonFatalError { fs.addDelegationTokens(renewer, creds) }
      }
    }

    creds
  }

  private def getTokenRenewalInterval(
      hadoopConf: Configuration,
      filesystems: Set[FileSystem]): Option[Long] = {
    // We cannot use the tokens generated with renewer yarn. Trying to renew
    // those will fail with an access control issue. So create new tokens with the logged in
    // user as renewer.
    val renewer = UserGroupInformation.getCurrentUser().getUserName()

    val creds = new Credentials()
    fetchDelegationTokens(renewer, filesystems, creds, Set.empty)

    val renewIntervals = creds.getAllTokens.asScala.filter {
      _.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier]
    }.flatMap { token =>
      Try {
        val newExpiration = token.renew(hadoopConf)
        val identifier = token.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
        val tokenKind = token.getKind.toString
        val interval = newExpiration - getIssueDate(tokenKind, identifier)
        logInfo(log"Renewal interval is ${MDC(TOTAL_TIME, interval)} for" +
          log" token ${MDC(TOKEN_KIND, tokenKind)}")
        token.cancel(hadoopConf)
        interval
      }.toOption
    }
    if (renewIntervals.isEmpty) None else Some(renewIntervals.min)
  }

  private def getIssueDate(kind: String, identifier: AbstractDelegationTokenIdentifier): Long = {
    val now = System.currentTimeMillis()
    val issueDate = identifier.getIssueDate
    if (issueDate > now) {
      logWarning(log"Token ${MDC(TOKEN_KIND, kind)} has set up issue date later than " +
        log"current time (provided: " +
        log"${MDC(ISSUE_DATE, issueDate)} / current timestamp: ${MDC(CURRENT_TIME, now)}). " +
        log"Please make sure clocks are in sync between " +
        log"machines. If the issue is not a clock mismatch, consult token implementor to check " +
        log"whether issue date is valid.")
      issueDate
    } else if (issueDate > 0L) {
      issueDate
    } else {
      logWarning(log"Token ${MDC(TOKEN_KIND, kind)} has not set up issue date properly " +
        log"(provided: ${MDC(ISSUE_DATE, issueDate)}). " +
        log"Using current timestamp (${MDC(CURRENT_TIME, now)} as issue date instead. " +
        log"Consult token implementor to fix the behavior.")
      now
    }
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
