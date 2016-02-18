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
package org.apache.spark.deploy.yarn

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.{ThreadUtils, Utils}

import scala.util.control.NonFatal

private[spark] class ExecutorDelegationTokenUpdater(
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends Logging {

  @volatile private var lastCredentialsFileSuffix = 0

  private val credentialsFile = sparkConf.get("spark.yarn.credentials.file")
  private val freshHadoopConf =
    SparkHadoopUtil.get.getConfBypassingFSCache(
      hadoopConf, new Path(credentialsFile).toUri.getScheme)

  private val delegationTokenRenewer =
    Executors.newSingleThreadScheduledExecutor(
      ThreadUtils.namedThreadFactory("Delegation Token Refresh Thread"))

  // On the executor, this thread wakes up and picks up new tokens from HDFS, if any.
  private val executorUpdaterRunnable =
    new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions(updateCredentialsIfRequired())
    }

  def updateCredentialsIfRequired(): Unit = {
    try {
      val credentialsFilePath = new Path(credentialsFile)
      val remoteFs = FileSystem.get(freshHadoopConf)
      SparkHadoopUtil.get.listFilesSorted(
        remoteFs, credentialsFilePath.getParent,
        credentialsFilePath.getName, SparkHadoopUtil.SPARK_YARN_CREDS_TEMP_EXTENSION)
        .lastOption.foreach { credentialsStatus =>
        val suffix = SparkHadoopUtil.get.getSuffixForCredentialsPath(credentialsStatus.getPath)
        if (suffix > lastCredentialsFileSuffix) {
          logInfo("Reading new delegation tokens from " + credentialsStatus.getPath)
          val newCredentials = getCredentialsFromHDFSFile(remoteFs, credentialsStatus.getPath)
          lastCredentialsFileSuffix = suffix
          UserGroupInformation.getCurrentUser.addCredentials(newCredentials)
          logInfo("Tokens updated from credentials file.")
        } else {
          // Check every hour to see if new credentials arrived.
          logInfo("Updated delegation tokens were expected, but the driver has not updated the " +
            "tokens yet, will check again in an hour.")
          delegationTokenRenewer.schedule(executorUpdaterRunnable, 1, TimeUnit.HOURS)
          return
        }
      }
      val timeFromNowToRenewal =
        SparkHadoopUtil.get.getTimeFromNowToRenewal(
          sparkConf, 0.8, UserGroupInformation.getCurrentUser.getCredentials)
      if (timeFromNowToRenewal <= 0) {
        // We just checked for new credentials but none were there, wait a minute and retry.
        // This handles the shutdown case where the staging directory may have been removed(see
        // SPARK-12316 for more details).
        delegationTokenRenewer.schedule(executorUpdaterRunnable, 1, TimeUnit.MINUTES)
      } else {
        logInfo(s"Scheduling token refresh from HDFS in $timeFromNowToRenewal millis.")
        delegationTokenRenewer.schedule(
          executorUpdaterRunnable, timeFromNowToRenewal, TimeUnit.MILLISECONDS)
      }
    } catch {
      // Since the file may get deleted while we are reading it, catch the Exception and come
      // back in an hour to try again
      case NonFatal(e) =>
        logWarning("Error while trying to update credentials, will try again in 1 hour", e)
        delegationTokenRenewer.schedule(executorUpdaterRunnable, 1, TimeUnit.HOURS)
    }
  }

  private def getCredentialsFromHDFSFile(remoteFs: FileSystem, tokenPath: Path): Credentials = {
    val stream = remoteFs.open(tokenPath)
    try {
      val newCredentials = new Credentials()
      newCredentials.readTokenStorageStream(stream)
      newCredentials
    } finally {
      stream.close()
    }
  }

  def stop(): Unit = {
    delegationTokenRenewer.shutdown()
  }

}
