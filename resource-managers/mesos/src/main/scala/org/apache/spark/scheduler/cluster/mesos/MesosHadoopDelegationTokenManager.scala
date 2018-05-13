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

package org.apache.spark.scheduler.cluster.mesos

import java.security.PrivilegedExceptionAction
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.ThreadUtils


/**
 * The MesosHadoopDelegationTokenManager fetches and updates Hadoop delegation tokens on the behalf
 * of the MesosCoarseGrainedSchedulerBackend. It is modeled after the YARN AMCredentialRenewer,
 * and similarly will renew the Credentials when 75% of the renewal interval has passed.
 * The principal difference is that instead of writing the new credentials to HDFS and
 * incrementing the timestamp of the file, the new credentials (called Tokens when they are
 * serialized) are broadcast to all running executors. On the executor side, when new Tokens are
 * received they overwrite the current credentials.
 */
private[spark] class MesosHadoopDelegationTokenManager(
    conf: SparkConf,
    hadoopConfig: Configuration,
    driverEndpoint: RpcEndpointRef)
  extends Logging {

  require(driverEndpoint != null, "DriverEndpoint is not initialized")

  private val credentialRenewerThread: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("Credential Renewal Thread")

  private val tokenManager: HadoopDelegationTokenManager =
    new HadoopDelegationTokenManager(conf, hadoopConfig)

  private val principal: String = conf.get(config.PRINCIPAL).orNull

  private var (tokens: Array[Byte], timeOfNextRenewal: Long) = {
    try {
      val creds = UserGroupInformation.getCurrentUser.getCredentials
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
      val rt = tokenManager.obtainDelegationTokens(hadoopConf, creds)
      logInfo(s"Initialized tokens: ${SparkHadoopUtil.get.dumpTokens(creds)}")
      (SparkHadoopUtil.get.serialize(creds), SparkHadoopUtil.nextCredentialRenewalTime(rt, conf))
    } catch {
      case e: Exception =>
        logError(s"Failed to fetch Hadoop delegation tokens $e")
        throw e
    }
  }

  private val keytabFile: Option[String] = conf.get(config.KEYTAB)

  scheduleTokenRenewal()

  private def scheduleTokenRenewal(): Unit = {
    if (keytabFile.isDefined) {
      require(principal != null, "Principal is required for Keytab-based authentication")
      logInfo(s"Using keytab: ${keytabFile.get} and principal $principal")
    } else {
      logInfo("Using ticket cache for Kerberos authentication, no token renewal.")
      return
    }

    def scheduleRenewal(runnable: Runnable): Unit = {
      val remainingTime = timeOfNextRenewal - System.currentTimeMillis()
      if (remainingTime <= 0) {
        logInfo("Credentials have expired, creating new ones now.")
        runnable.run()
      } else {
        logInfo(s"Scheduling login from keytab in $remainingTime millis.")
        credentialRenewerThread.schedule(runnable, remainingTime, TimeUnit.MILLISECONDS)
      }
    }

    val credentialRenewerRunnable =
      new Runnable {
        override def run(): Unit = {
          try {
            getNewDelegationTokens()
            broadcastDelegationTokens(tokens)
          } catch {
            case e: Exception =>
              // Log the error and try to write new tokens back in an hour
              val delay = TimeUnit.SECONDS.toMillis(conf.get(config.CREDENTIALS_RENEWAL_RETRY_WAIT))
              logWarning(
                s"Couldn't broadcast tokens, trying again in ${UIUtils.formatDuration(delay)}", e)
              credentialRenewerThread.schedule(this, delay, TimeUnit.MILLISECONDS)
              return
          }
          scheduleRenewal(this)
        }
      }
    scheduleRenewal(credentialRenewerRunnable)
  }

  private def getNewDelegationTokens(): Unit = {
    logInfo(s"Attempting to login to KDC with principal ${principal}")
    // Get new delegation tokens by logging in with a new UGI inspired by AMCredentialRenewer.scala
    // Don't protect against keytabFile being empty because it's guarded above.
    val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabFile.get)
    logInfo("Successfully logged into KDC")
    val tempCreds = ugi.getCredentials
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val nextRenewalTime = ugi.doAs(new PrivilegedExceptionAction[Long] {
      override def run(): Long = {
        tokenManager.obtainDelegationTokens(hadoopConf, tempCreds)
      }
    })

    val currTime = System.currentTimeMillis()
    timeOfNextRenewal = if (nextRenewalTime <= currTime) {
      logWarning(s"Next credential renewal time ($nextRenewalTime) is earlier than " +
        s"current time ($currTime), which is unexpected, please check your credential renewal " +
        "related configurations in the target services.")
      currTime
    } else {
      SparkHadoopUtil.nextCredentialRenewalTime(nextRenewalTime, conf)
    }
    logInfo(s"Time of next renewal is in ${timeOfNextRenewal - System.currentTimeMillis()} ms")

    // Add the temp credentials back to the original ones.
    UserGroupInformation.getCurrentUser.addCredentials(tempCreds)
    // update tokens for late or dynamically added executors
    tokens = SparkHadoopUtil.get.serialize(tempCreds)
  }

  private def broadcastDelegationTokens(tokens: Array[Byte]) = {
    logInfo("Sending new tokens to all executors")
    driverEndpoint.send(UpdateDelegationTokens(tokens))
  }

  def getTokens(): Array[Byte] = {
    tokens
  }
}

