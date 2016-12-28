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

import java.security.PrivilegedExceptionAction
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.ThreadUtils

/**
 * The following methods are primarily meant to make sure long-running apps like Spark
 * Streaming apps can run without interruption while accessing secured services. The
 * scheduleLoginFromKeytab method is called on the AM to get the new credentials.
 * This method wakes up a thread that logs into the KDC
 * once 75% of the renewal interval of the original credentials used for the container
 * has elapsed. It then obtains new credentials and writes them to HDFS in a
 * pre-specified location - the prefix of which is specified in the sparkConf by
 * spark.yarn.credentials.file (so the file(s) would be named c-timestamp1-1, c-timestamp2-2 etc.
 * - each update goes to a new file, with a monotonically increasing suffix), also the
 * timestamp1, timestamp2 here indicates the time of next update for CredentialUpdater.
 * After this, the credentials are renewed once 75% of the new tokens renewal interval has elapsed.
 *
 * On the executor and driver (yarn client mode) side, the updateCredentialsIfRequired method is
 * called once 80% of the validity of the original credentials has elapsed. At that time the
 * executor finds the credentials file with the latest timestamp and checks if it has read those
 * credentials before (by keeping track of the suffix of the last file it read). If a new file has
 * appeared, it will read the credentials and update the currently running UGI with it. This
 * process happens again once 80% of the validity of this has expired.
 */
private[yarn] class AMCredentialRenewer(
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    credentialManager: ConfigurableCredentialManager) extends Logging {

  private var lastCredentialsFileSuffix = 0

  private val credentialRenewer =
    Executors.newSingleThreadScheduledExecutor(
      ThreadUtils.namedThreadFactory("Credential Refresh Thread"))

  private val hadoopUtil = YarnSparkHadoopUtil.get

  private val credentialsFile = sparkConf.get(CREDENTIALS_FILE_PATH)
  private val daysToKeepFiles = sparkConf.get(CREDENTIALS_FILE_MAX_RETENTION)
  private val numFilesToKeep = sparkConf.get(CREDENTIAL_FILE_MAX_COUNT)
  private val freshHadoopConf =
    hadoopUtil.getConfBypassingFSCache(hadoopConf, new Path(credentialsFile).toUri.getScheme)

  @volatile private var timeOfNextRenewal = sparkConf.get(CREDENTIALS_RENEWAL_TIME)

  /**
   * Schedule a login from the keytab and principal set using the --principal and --keytab
   * arguments to spark-submit. This login happens only when the credentials of the current user
   * are about to expire. This method reads spark.yarn.principal and spark.yarn.keytab from
   * SparkConf to do the login. This method is a no-op in non-YARN mode.
   *
   */
  private[spark] def scheduleLoginFromKeytab(): Unit = {
    val principal = sparkConf.get(PRINCIPAL).get
    val keytab = sparkConf.get(KEYTAB).get

    /**
     * Schedule re-login and creation of new credentials. If credentials have already expired, this
     * method will synchronously create new ones.
     */
    def scheduleRenewal(runnable: Runnable): Unit = {
      // Run now!
      val remainingTime = timeOfNextRenewal - System.currentTimeMillis()
      if (remainingTime <= 0) {
        logInfo("Credentials have expired, creating new ones now.")
        runnable.run()
      } else {
        logInfo(s"Scheduling login from keytab in $remainingTime millis.")
        credentialRenewer.schedule(runnable, remainingTime, TimeUnit.MILLISECONDS)
      }
    }

    // This thread periodically runs on the AM to update the credentials on HDFS.
    val credentialRenewerRunnable =
      new Runnable {
        override def run(): Unit = {
          try {
            writeNewCredentialsToHDFS(principal, keytab)
            cleanupOldFiles()
          } catch {
            case e: Exception =>
              // Log the error and try to write new tokens back in an hour
              logWarning("Failed to write out new credentials to HDFS, will try again in an " +
                "hour! If this happens too often tasks will fail.", e)
              credentialRenewer.schedule(this, 1, TimeUnit.HOURS)
              return
          }
          scheduleRenewal(this)
        }
      }
    // Schedule update of credentials. This handles the case of updating the credentials right now
    // as well, since the renewal interval will be 0, and the thread will get scheduled
    // immediately.
    scheduleRenewal(credentialRenewerRunnable)
  }

  // Keeps only files that are newer than daysToKeepFiles days, and deletes everything else. At
  // least numFilesToKeep files are kept for safety
  private def cleanupOldFiles(): Unit = {
    import scala.concurrent.duration._
    try {
      val remoteFs = FileSystem.get(freshHadoopConf)
      val credentialsPath = new Path(credentialsFile)
      val thresholdTime = System.currentTimeMillis() - (daysToKeepFiles.days).toMillis
      hadoopUtil.listFilesSorted(
        remoteFs, credentialsPath.getParent,
        credentialsPath.getName, SparkHadoopUtil.SPARK_YARN_CREDS_TEMP_EXTENSION)
        .dropRight(numFilesToKeep)
        .takeWhile(_.getModificationTime < thresholdTime)
        .foreach(x => remoteFs.delete(x.getPath, true))
    } catch {
      // Such errors are not fatal, so don't throw. Make sure they are logged though
      case e: Exception =>
        logWarning("Error while attempting to cleanup old credentials. If you are seeing many " +
          "such warnings there may be an issue with your HDFS cluster.", e)
    }
  }

  private def writeNewCredentialsToHDFS(principal: String, keytab: String): Unit = {
    // Keytab is copied by YARN to the working directory of the AM, so full path is
    // not needed.

    // HACK:
    // HDFS will not issue new delegation tokens, if the Credentials object
    // passed in already has tokens for that FS even if the tokens are expired (it really only
    // checks if there are tokens for the service, and not if they are valid). So the only real
    // way to get new tokens is to make sure a different Credentials object is used each time to
    // get new tokens and then the new tokens are copied over the current user's Credentials.
    // So:
    // - we login as a different user and get the UGI
    // - use that UGI to get the tokens (see doAs block below)
    // - copy the tokens over to the current user's credentials (this will overwrite the tokens
    // in the current user's Credentials object for this FS).
    // The login to KDC happens each time new tokens are required, but this is rare enough to not
    // have to worry about (like once every day or so). This makes this code clearer than having
    // to login and then relogin every time (the HDFS API may not relogin since we don't use this
    // UGI directly for HDFS communication.
    logInfo(s"Attempting to login to KDC using principal: $principal")
    val keytabLoggedInUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
    logInfo("Successfully logged into KDC.")
    val tempCreds = keytabLoggedInUGI.getCredentials
    val credentialsPath = new Path(credentialsFile)
    val dst = credentialsPath.getParent
    var nearestNextRenewalTime = Long.MaxValue
    keytabLoggedInUGI.doAs(new PrivilegedExceptionAction[Void] {
      // Get a copy of the credentials
      override def run(): Void = {
        nearestNextRenewalTime = credentialManager.obtainCredentials(freshHadoopConf, tempCreds)
        null
      }
    })

    val currTime = System.currentTimeMillis()
    val timeOfNextUpdate = if (nearestNextRenewalTime <= currTime) {
      // If next renewal time is earlier than current time, we set next renewal time to current
      // time, this will trigger next renewal immediately. Also set next update time to current
      // time. There still has a gap between token renewal and update will potentially introduce
      // issue.
      logWarning(s"Next credential renewal time ($nearestNextRenewalTime) is earlier than " +
        s"current time ($currTime), which is unexpected, please check your credential renewal " +
        "related configurations in the target services.")
      timeOfNextRenewal = currTime
      currTime
    } else {
      // Next valid renewal time is about 75% of credential renewal time, and update time is
      // slightly later than valid renewal time (80% of renewal time).
      timeOfNextRenewal = ((nearestNextRenewalTime - currTime) * 0.75 + currTime).toLong
      ((nearestNextRenewalTime - currTime) * 0.8 + currTime).toLong
    }

    // Add the temp credentials back to the original ones.
    UserGroupInformation.getCurrentUser.addCredentials(tempCreds)
    val remoteFs = FileSystem.get(freshHadoopConf)
    // If lastCredentialsFileSuffix is 0, then the AM is either started or restarted. If the AM
    // was restarted, then the lastCredentialsFileSuffix might be > 0, so find the newest file
    // and update the lastCredentialsFileSuffix.
    if (lastCredentialsFileSuffix == 0) {
      hadoopUtil.listFilesSorted(
        remoteFs, credentialsPath.getParent,
        credentialsPath.getName, SparkHadoopUtil.SPARK_YARN_CREDS_TEMP_EXTENSION)
        .lastOption.foreach { status =>
        lastCredentialsFileSuffix = hadoopUtil.getSuffixForCredentialsPath(status.getPath)
      }
    }
    val nextSuffix = lastCredentialsFileSuffix + 1

    val tokenPathStr =
      credentialsFile + SparkHadoopUtil.SPARK_YARN_CREDS_COUNTER_DELIM +
        timeOfNextUpdate.toLong.toString + SparkHadoopUtil.SPARK_YARN_CREDS_COUNTER_DELIM +
          nextSuffix
    val tokenPath = new Path(tokenPathStr)
    val tempTokenPath = new Path(tokenPathStr + SparkHadoopUtil.SPARK_YARN_CREDS_TEMP_EXTENSION)

    logInfo("Writing out delegation tokens to " + tempTokenPath.toString)
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    credentials.writeTokenStorageFile(tempTokenPath, freshHadoopConf)
    logInfo(s"Delegation Tokens written out successfully. Renaming file to $tokenPathStr")
    remoteFs.rename(tempTokenPath, tokenPath)
    logInfo("Delegation token file rename complete.")
    lastCredentialsFileSuffix = nextSuffix
  }

  def stop(): Unit = {
    credentialRenewer.shutdown()
  }
}
