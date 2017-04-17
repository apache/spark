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

package org.apache.spark.deploy.yarn.security

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.security.HadoopFSCredentialProvider
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.config._

class YARNHadoopFSCredentialProvider extends HadoopFSCredentialProvider {

  override def getTokenRenewer(conf: Configuration): String = {
    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    logDebug("delegation token renewer is: " + delegTokenRenewer)
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer"
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }

    delegTokenRenewer
  }

  override def hadoopFSsToAccess(hadoopConf: Configuration, sparkConf: SparkConf): Set[Path] = {
    sparkConf.get(FILESYSTEMS_TO_ACCESS).map(new Path(_)).toSet +
      sparkConf.get(STAGING_DIR).map(new Path(_))
        .getOrElse(FileSystem.get(hadoopConf).getHomeDirectory)
  }

  override def getTokenRenewalInterval(
    hadoopConf: Configuration, sparkConf: SparkConf): Option[Long] = {
    // We cannot use the tokens generated with renewer yarn. Trying to renew
    // those will fail with an access control issue. So create new tokens with the logged in
    // user as renewer.
    sparkConf.get(PRINCIPAL).flatMap { renewer =>
      val creds = new Credentials()
      hadoopFSsToAccess(hadoopConf, sparkConf).foreach { dst =>
        val dstFs = dst.getFileSystem(hadoopConf)
        dstFs.addDelegationTokens(renewer, creds)
      }

      val renewIntervals = creds.getAllTokens.asScala.filter {
        _.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier]
      }.flatMap { token =>
        Try {
          val newExpiration = token.renew(hadoopConf)
          val identifier = token.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
          val interval = newExpiration - identifier.getIssueDate
          logInfo(s"Renewal interval is $interval for token ${token.getKind.toString}")
          interval
        }.toOption
      }
      if (renewIntervals.isEmpty) None else Some(renewIntervals.min)
    }
  }

}
