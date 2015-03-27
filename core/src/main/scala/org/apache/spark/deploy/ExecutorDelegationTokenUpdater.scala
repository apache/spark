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
package org.apache.spark.deploy

import java.util.concurrent.{Executors, TimeUnit}
import java.util.{Comparator, Arrays}

import com.google.common.primitives.Longs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{PathFilter, FileStatus, Path, FileSystem}
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf}

private[spark] class ExecutorDelegationTokenUpdater(
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends Logging {

  @volatile private var lastCredentialsFileSuffix = 0

  private lazy val delegationTokenRenewer =
    Executors.newSingleThreadScheduledExecutor(
      Utils.namedThreadFactory("Delegation Token Refresh Thread"))

  // On the executor, this thread wakes up and picks up new tokens from HDFS, if any.
  private lazy val executorUpdaterRunnable =
    new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions(updateCredentialsIfRequired())
    }

  def updateCredentialsIfRequired(): Unit = {
    try {
      sparkConf.getOption("spark.yarn.credentials.file").foreach { credentialsFile =>
        val credentials = UserGroupInformation.getCurrentUser.getCredentials
        val credentialsFilePath = new Path(credentialsFile)
        val remoteFs = FileSystem.get(hadoopConf)
        SparkHadoopUtil.get.listFilesSorted(
          remoteFs, credentialsFilePath.getParent, credentialsFilePath.getName, ".tmp")
          .lastOption.foreach { credentialsStatus =>
          val suffix = getSuffixForCredentialsPath(credentialsStatus)
          if (suffix > lastCredentialsFileSuffix) {
            logInfo("Reading new delegation tokens from " + credentialsStatus.getPath)
            val newCredentials = getCredentialsFromHDFSFile(remoteFs, credentialsStatus.getPath)
            lastCredentialsFileSuffix = suffix
            UserGroupInformation.getCurrentUser.addCredentials(newCredentials)
            val totalValidity = SparkHadoopUtil.get.getLatestTokenValidity(credentials) -
              credentialsStatus.getModificationTime
            val timeToRunRenewal =
              credentialsStatus.getModificationTime + (0.8 * totalValidity).toLong
            val timeFromNowToRenewal = timeToRunRenewal - System.currentTimeMillis()
            logInfo("Updated delegation tokens, will check for new tokens in " +
              timeFromNowToRenewal + " millis")
            delegationTokenRenewer.schedule(
              executorUpdaterRunnable, timeFromNowToRenewal, TimeUnit.MILLISECONDS)
          } else {
            // Check every hour to see if new credentials arrived.
            logInfo("Updated delegation tokens were expected, but the driver has not updated the " +
              "tokens yet, will check again in an hour.")
            delegationTokenRenewer.schedule(executorUpdaterRunnable, 1, TimeUnit.HOURS)
          }
        }
      }
    } catch {
      // Since the file may get deleted while we are reading it, catch the Exception and come
      // back in an hour to try again
      case e: Exception =>
        logWarning("Error while trying to update credentials, will try again in 1 hour", e)
        delegationTokenRenewer.schedule(executorUpdaterRunnable, 1, TimeUnit.HOURS)
    }
  }

  private def getCredentialsFromHDFSFile(
    remoteFs: FileSystem,
    tokenPath: Path): Credentials = {
    val stream = remoteFs.open(tokenPath)
    try {
      val newCredentials = new Credentials()
      newCredentials.readFields(stream)
      newCredentials
    } finally {
      stream.close()
    }
  }

  def stop(): Unit = {
    delegationTokenRenewer.shutdown()
  }

  private def getSuffixForCredentialsPath(credentialsStatus: FileStatus): Int = {
    val fileName = credentialsStatus.getPath.getName
    fileName.substring(fileName.lastIndexOf("-") + 1).toInt
  }
}
