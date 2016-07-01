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

package org.apache.spark.deploy.yarn.token

import java.io.{ByteArrayInputStream, DataInputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.Token

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

private[yarn] class HDFSTokenProvider extends ServiceTokenProvider with Logging {

  private var nnsToAccess: Set[Path] = Set.empty
  private var tokenRenewer: Option[String] = None

  override val  serviceName: String = "hdfs"

  override def obtainTokensFromService(
      sparkConf: SparkConf,
      serviceConf: Configuration,
      creds: Credentials)
    : Array[Token[_]] = {
    val tokens = ArrayBuffer[Token[_]]()
    val renewer = tokenRenewer.getOrElse(getTokenRenewer(serviceConf))
    nnsToAccess.foreach { dst =>
      val dstFs = dst.getFileSystem(serviceConf)
      logInfo("getting token for namenode: " + dst)
      tokens  ++= dstFs.addDelegationTokens(renewer, creds)
    }

    tokens.toArray
  }

  override def getTokenRenewalInterval(sparkConf: SparkConf, serviceConf: Configuration): Long = {
    // We cannot use the tokens generated above since those have renewer yarn. Trying to renew
    // those will fail with an access control issue. So create new tokens with the logged in
    // user as renewer.
    val creds = new Credentials()
    obtainTokensFromService(sparkConf, serviceConf, creds)
    val t = creds.getAllTokens.asScala
      .filter(_.getKind == DelegationTokenIdentifier.HDFS_DELEGATION_KIND)
      .head
    val newExpiration = t.renew(serviceConf)
    val identifier = new DelegationTokenIdentifier()
    identifier.readFields(new DataInputStream(new ByteArrayInputStream(t.getIdentifier)))
    val interval = newExpiration - identifier.getIssueDate
    logInfo(s"Renewal Interval set to $interval")
    interval
  }

  override def getTimeFromNowToRenewal(
      sparkConf: SparkConf,
      fraction: Double,
      credentials: Credentials): Long = {
    val now = System.currentTimeMillis()

    val renewalInterval = sparkConf.get(TOKEN_RENEWAL_INTERVAL).get

    credentials.getAllTokens.asScala
      .filter(_.getKind == DelegationTokenIdentifier.HDFS_DELEGATION_KIND)
      .map { t =>
        val identifier = new DelegationTokenIdentifier()
        identifier.readFields(new DataInputStream(new ByteArrayInputStream(t.getIdentifier)))
        (identifier.getIssueDate + fraction * renewalInterval).toLong - now
      }.foldLeft(0L)(math.max)
  }

  def setNameNodesToAccess(sparkConf: SparkConf, dirs: Set[Path]): Unit = {
    nnsToAccess = sparkConf.get(NAMENODES_TO_ACCESS)
      .map(new Path(_))
      .toSet ++ dirs
  }

  def setTokenRenewer(tokenRenewer: String): Unit = {
    this.tokenRenewer = Some(tokenRenewer)
  }

  private def getTokenRenewer(conf: Configuration): String = {
    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    logDebug("delegation token renewer is: " + delegTokenRenewer)
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer"
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }

    delegTokenRenewer
  }
}
