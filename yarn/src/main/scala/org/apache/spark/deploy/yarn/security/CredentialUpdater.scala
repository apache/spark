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

import java.util.concurrent.{Executors, TimeUnit}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class CredentialUpdater(
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    credentialManager: ConfigurableCredentialManager) extends Logging {

  @volatile private var lastCredentialsFileSuffix = 0

  private val credentialsFile = sparkConf.get(CREDENTIALS_FILE_PATH)
  private val freshHadoopConf =
    SparkHadoopUtil.get.getConfBypassingFSCache(
      hadoopConf, new Path(credentialsFile).toUri.getScheme)

  private val credentialUpdater =
    Executors.newSingleThreadScheduledExecutor(
      ThreadUtils.namedThreadFactory("Credential Refresh Thread"))

  // This thread wakes up and picks up new credentials from HDFS, if any.
  private val credentialUpdaterRunnable =
    new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions(timelyUpdateCredentialsIfRequired())
    }

  /** Start the credential updater task */
  def start(): Unit = {
    val startTime = sparkConf.get(CREDENTIALS_RENEWAL_TIME)
    val remainingTime = startTime - System.currentTimeMillis()
    if (remainingTime <= 0) {
      credentialUpdater.schedule(credentialUpdaterRunnable, 1, TimeUnit.MINUTES)
    } else {
      logInfo(s"Scheduling credentials refresh from HDFS in $remainingTime millis.")
      credentialUpdater.schedule(credentialUpdaterRunnable, remainingTime, TimeUnit.MILLISECONDS)
    }
  }

  def updateCredentials(): Long = {
    try {
      val credentialsFilePath = new Path(credentialsFile)
      val remoteFs = FileSystem.get(freshHadoopConf)
      SparkHadoopUtil.get.listFilesSorted(
        remoteFs, credentialsFilePath.getParent,
        credentialsFilePath.getName, SparkHadoopUtil.SPARK_YARN_CREDS_TEMP_EXTENSION)
        .lastOption.map { credentialsStatus =>
          val suffix = SparkHadoopUtil.get.getSuffixForCredentialsPath(credentialsStatus.getPath)
          val timeOfNextUpdate = getTimeOfNextUpdateFromFileName(credentialsStatus.getPath)
          val currTime = System.currentTimeMillis()
          if (suffix > lastCredentialsFileSuffix) {
            logInfo("Reading new credentials from " + credentialsStatus.getPath)
            val newCredentials = getCredentialsFromHDFSFile(remoteFs, credentialsStatus.getPath)
            lastCredentialsFileSuffix = suffix
            UserGroupInformation.getCurrentUser.addCredentials(newCredentials)
            logInfo("Credentials updated from credentials file.")

            val remainingTime = timeOfNextUpdate - currTime
            if (remainingTime <= 0) TimeUnit.MINUTES.toMillis(1) else remainingTime
          } else if (timeOfNextUpdate > currTime) {
            // Current suffix is older than expected but credential update time is later than
            // current timestamp.
            // This is due to manual credential update called by user explicitly.
            timeOfNextUpdate - currTime
          } else {
            TimeUnit.HOURS.toMillis(1)
          }
      }.getOrElse {
        // Wait for 1 minute to check again if there's no credential file currently
        TimeUnit.MINUTES.toMillis(1)
      }
    } catch {
      // Since the file may get deleted while we are reading it, catch the Exception and come
      // back in an hour to try again
      case NonFatal(e) =>
        logWarning("Error while trying to update credentials, will try again in 1 hour", e)
        TimeUnit.HOURS.toMillis(1)
    }
  }

  private def timelyUpdateCredentialsIfRequired(): Unit = {
    val timeToNextUpdate = updateCredentials()
    credentialUpdater.schedule(
      credentialUpdaterRunnable, timeToNextUpdate, TimeUnit.MILLISECONDS)
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

  private def getTimeOfNextUpdateFromFileName(credentialsPath: Path): Long = {
    val name = credentialsPath.getName
    val index = name.lastIndexOf(SparkHadoopUtil.SPARK_YARN_CREDS_COUNTER_DELIM)
    val slice = name.substring(0, index)
    val last2index = slice.lastIndexOf(SparkHadoopUtil.SPARK_YARN_CREDS_COUNTER_DELIM)
    name.substring(last2index + 1, index).toLong
  }

  def stop(): Unit = {
    credentialUpdater.shutdown()
  }

}
