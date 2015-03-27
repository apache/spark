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

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf}

/*
 * The following methods are primarily meant to make sure long-running apps like Spark
 * Streaming apps can run without interruption while writing to secure HDFS. The
 * scheduleLoginFromKeytab method is called on the driver when the
 * CoarseGrainedScheduledBackend starts up. This method wakes up a thread that logs into the KDC
 * once 75% of the expiry time of the original delegation tokens used for the container
 * has elapsed. It then creates new delegation tokens and writes them to HDFS in a
 * pre-specified location - the prefix of which is specified in the sparkConf by
 * spark.yarn.credentials.file (so the file(s) would be named c-1, c-2 etc. - each update goes
 * to a new file, with a monotonically increasing suffix). After this, the credentials are
 * updated once 75% of the new tokens validity has elapsed.
 *
 * On the executor side, the updateCredentialsIfRequired method is called once 80% of the
 * validity of the original tokens has elapsed. At that time the executor finds the
 * credentials file with the latest timestamp and checks if it has read those credentials
 * before (by keeping track of the suffix of the last file it read). If a new file has
 * appeared, it will read the credentials and update the currently running UGI with it. This
 * process happens again once 80% of the validity of this has expired.
 */
class AMDelegationTokenRenewer(sparkConf: SparkConf, hadoopConf: Configuration) extends Logging {

  private var lastCredentialsFileSuffix = 0

  private lazy val delegationTokenRenewer =
    Executors.newSingleThreadScheduledExecutor(
      Utils.namedThreadFactory("Delegation Token Refresh Thread"))

  private var loggedInViaKeytab = false
  private var loggedInUGI: UserGroupInformation = null

  private lazy val hadoopUtil = YarnSparkHadoopUtil.get

  /**
   * Schedule a login from the keytab and principal set using the --principal and --keytab
   * arguments to spark-submit. This login happens only when the credentials of the current user
   * are about to expire. This method reads SPARK_PRINCIPAL and SPARK_KEYTAB from the environment
   * to do the login. This method is a no-op in non-YARN mode.
   */
  private[spark] def scheduleLoginFromKeytab(): Unit = {
    sparkConf.getOption("spark.yarn.principal").foreach { principal =>
      val keytab = sparkConf.get("spark.yarn.keytab")

      def getRenewalInterval = {
        val credentials = UserGroupInformation.getCurrentUser.getCredentials
        math.max((0.75 * (hadoopUtil.getLatestTokenValidity(credentials) -
          System.currentTimeMillis())).toLong, 0L)
      }

      def scheduleRenewal(runnable: Runnable) = {
        val renewalInterval = getRenewalInterval
        logInfo(s"Scheduling login from keytab in $renewalInterval millis.")
        delegationTokenRenewer.schedule(runnable, renewalInterval, TimeUnit.MILLISECONDS)
      }

      // This thread periodically runs on the driver to update the delegation tokens on HDFS.
      val driverTokenRenewerRunnable =
        new Runnable {
          override def run(): Unit = {
            try {
              writeNewTokensToHDFS(principal, keytab)
            } catch {
              case e: Exception =>
                logWarning("Failed to write out new credentials to HDFS, will try again in an " +
                  "hour! If this happens too often tasks will fail.", e)
                delegationTokenRenewer.schedule(this, 1, TimeUnit.HOURS)
                return
            }
            cleanupOldFiles()
            scheduleRenewal(this)
          }
        }
      // If this is an AM restart, it is possible that the original tokens have expired, which
      // means we need to login immediately to get new tokens.
      if (getRenewalInterval == 0) writeNewTokensToHDFS(principal, keytab)
      // Schedule update of credentials
      scheduleRenewal(driverTokenRenewerRunnable)

    }
  }

  // Keeps only files that are newer than 30 days, and deletes everything else. At least 5 files
  // are kept for safety
  private def cleanupOldFiles(): Unit = {
    import scala.concurrent.duration._
    try {
      val remoteFs = FileSystem.get(hadoopConf)
      val credentialsPath = new Path(sparkConf.get("spark.yarn.credentials.file"))
      hadoopUtil.listFilesSorted(
        remoteFs, credentialsPath.getParent, credentialsPath.getName, ".tmp").dropRight(5)
        .takeWhile(_.getModificationTime < System.currentTimeMillis() - (30 days).toMillis)
        .foreach(x => remoteFs.delete(x.getPath, true))
    } catch {
      // Such errors are not fatal, so don't throw. Make sure they are logged though
      case e: Exception =>
        logWarning("Error while attempting to cleanup old tokens. If you are seeing many such " +
          "warnings there may be an issue with your HDFS cluster.")
    }
  }

  private def writeNewTokensToHDFS(principal: String, keytab: String): Unit = {
    if (!loggedInViaKeytab) {
      // Keytab is copied by YARN to the working directory of the AM, so full path is
      // not needed.
      logInfo(s"Attempting to login to KDC using principal: $principal")
      loggedInUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        principal, keytab)
      logInfo("Successfully logged into KDC.")
      loggedInViaKeytab = true
      // Not exactly sure when HDFS re-logs in, be safe and do it ourselves.
      // Periodically check and relogin this keytab. The UGI will take care of not relogging in
      // if it is not necessary to relogin.
      val reloginRunnable = new Runnable {
        override def run(): Unit = {
          try {
            loggedInUGI.checkTGTAndReloginFromKeytab()
          } catch {
            case e: Exception =>
              logError("Error while attempting tp relogin to KDC", e)
          }
        }
      }
      delegationTokenRenewer.schedule(reloginRunnable, 6, TimeUnit.HOURS)
    }
    val nns = YarnSparkHadoopUtil.get.getNameNodesToAccess(sparkConf)
    YarnSparkHadoopUtil.get.obtainTokensForNamenodes(nns, hadoopConf, loggedInUGI.getCredentials)
    val remoteFs = FileSystem.get(hadoopConf)
    // If lastCredentialsFileSuffix is 0, then the AM is either started or restarted. If the AM
    // was restarted, then the lastCredentialsFileSuffix might be > 0, so find the newest file
    // and update the lastCredentialsFileSuffix.
    if (lastCredentialsFileSuffix == 0) {
      val credentialsPath = new Path(sparkConf.get("spark.yarn.credentials.file"))
      SparkHadoopUtil.get.listFilesSorted(
        remoteFs, credentialsPath.getParent, credentialsPath.getName, ".tmp")
        .lastOption.foreach { status =>
        lastCredentialsFileSuffix = getSuffixForCredentialsPath(status)
      }
    }
    val nextSuffix = lastCredentialsFileSuffix + 1
    val tokenPathStr =
      sparkConf.get("spark.yarn.credentials.file") + "-" + nextSuffix
    val tokenPath = new Path(tokenPathStr)
    val tempTokenPath = new Path(tokenPathStr + ".tmp")
    logInfo("Writing out delegation tokens to " + tempTokenPath.toString)
    val stream = Option(remoteFs.create(tempTokenPath, true))
    try {
      stream.foreach { s =>
        loggedInUGI.getCredentials.writeTokenStorageToStream(s)
        s.hflush()
        s.close()
        logInfo(s"Delegation Tokens written out successfully. Renaming file to $tokenPathStr")

        remoteFs.rename(tempTokenPath, tokenPath)
        logInfo("Delegation token file rename complete.")
      }
    } finally {
      stream.foreach(_.close())
    }
    lastCredentialsFileSuffix = nextSuffix
  }

  def stop(): Unit = {
    delegationTokenRenewer.shutdown()
  }

  private def getSuffixForCredentialsPath(credentialsStatus: FileStatus): Int = {
    val fileName = credentialsStatus.getPath.getName
    fileName.substring(fileName.lastIndexOf("-") + 1).toInt
  }

}
