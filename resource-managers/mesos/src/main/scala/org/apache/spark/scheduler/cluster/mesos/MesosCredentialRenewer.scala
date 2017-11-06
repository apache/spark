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

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens
import org.apache.spark.util.ThreadUtils


/**
 * The MesosCredentialRenewer will update the Hadoop credentials for Spark drivers accessing
 * secured services using Kerberos authentication. It is modeled after the YARN AMCredential
 * renewer, and similarly will renew the Credentials when 75% of the renewal interval has passed.
 * The principal difference is that instead of writing the new credentials to HDFS and
 * incrementing the timestamp of the file, the new credentials (called Tokens when they are
 * serialized) are broadcast to all running executors. On the executor side, when new Tokens are
 * recieved they overwrite the current credentials.
 */
class MesosCredentialRenewer(
    conf: SparkConf,
    tokenManager: HadoopDelegationTokenManager) extends Logging {

  private val credentialRenewerThread: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("Credential Renewal Thread")

  private val principal = conf.get(config.PRINCIPAL).orNull

  private val (secretFile, mode) = getSecretFile(conf)

  var (tokens: Array[Byte], timeOfNextRenewal: Long) = {
    try {
      val creds = UserGroupInformation.getCurrentUser.getCredentials
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
      val rt = tokenManager.obtainDelegationTokens(hadoopConf, creds)
      (SparkHadoopUtil.get.serialize(creds), rt)
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to initialize Hadoop delegation tokens\n" +
          s"\tPricipal: $principal\n\tmode: $mode\n\tsecret file $secretFile\n\tException: $e")
    }

  }

  private def getSecretFile(conf: SparkConf): (String, String) = {
    val keytab = conf.get(config.KEYTAB).orNull
    val tgt = conf.getenv("KRB5CCNAME")
    require(keytab != null || tgt != null, "A keytab or TGT required.")
    // if both Keytab and TGT are detected we use the Keytab.
    val (secretFile, mode) = if (keytab != null && tgt != null) {
      logWarning(s"Keytab and TGT were detected, using keytab, " +
        s"unset ${config.KEYTAB.key} to use TGT")
      (keytab, "keytab")
    } else {
      val m = if (keytab != null) "keytab" else "tgt"
      val sf = if (keytab != null) keytab else tgt
      (sf, m)
    }

    if (principal == null) {
      logInfo(s"Using mode: $mode to retrieve Hadoop delegation tokens")
    } else {
      logInfo(s"Using principal: $principal with mode: $mode to retrieve Hadoop delegation tokens")
    }

    logDebug(s"secretFile is $secretFile")
    (secretFile, mode)
  }

  def scheduleTokenRenewal(driverEndpoint: RpcEndpointRef): Unit = {
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
            val tokensBytes = getNewDelegationTokens
            broadcastDelegationTokens(tokensBytes, driverEndpoint)
          } catch {
            case e: Exception =>
              // Log the error and try to write new tokens back in an hour
              logWarning("Couldn't broadcast tokens, trying again in an hour", e)
              credentialRenewerThread.schedule(this, 1, TimeUnit.HOURS)
              return
          }
          scheduleRenewal(this)
        }
      }
    scheduleRenewal(credentialRenewerRunnable)
  }

  private def getNewDelegationTokens: Array[Byte] = {
    logInfo(s"Attempting to login to KDC with ${conf.get(config.PRINCIPAL).orNull}")
    // Get new delegation tokens by logging in with a new UGI
    // inspired by AMCredentialRenewer.scala:L174
    val ugi = if (mode == "keytab") {
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, secretFile)
    } else {
      UserGroupInformation.getUGIFromTicketCache(secretFile, principal)
    }
    logInfo("Successfully logged into KDC")

    val tempCreds = ugi.getCredentials
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    var nextRenewalTime = Long.MaxValue
    ugi.doAs(new PrivilegedExceptionAction[Void] {
      override def run(): Void = {
        nextRenewalTime = tokenManager.obtainDelegationTokens(hadoopConf, tempCreds)
        null
      }
    })

    val currTime = System.currentTimeMillis()
    timeOfNextRenewal = if (nextRenewalTime <= currTime) {
      logWarning(s"Next credential renewal time ($nextRenewalTime) is earlier than " +
        s"current time ($currTime), which is unexpected, please check your credential renewal " +
        "related configurations in the target services.")
      currTime
    } else {
      SparkHadoopUtil.getDateOfNextUpdate(nextRenewalTime, 0.75)
    }
    logInfo(s"Time of next renewal is $timeOfNextRenewal")

    // Add the temp credentials back to the original ones.
    UserGroupInformation.getCurrentUser.addCredentials(tempCreds)
    SparkHadoopUtil.get.serialize(tempCreds)
  }

  private def broadcastDelegationTokens(tokens: Array[Byte], driverEndpoint: RpcEndpointRef) = {
    logInfo("Sending new tokens to all executors")
    driverEndpoint.send(UpdateDelegationTokens(tokens))
  }
}

