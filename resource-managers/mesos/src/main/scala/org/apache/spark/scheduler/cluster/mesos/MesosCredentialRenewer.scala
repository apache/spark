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
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens
import org.apache.spark.util.ThreadUtils


class MesosCredentialRenewer(
    conf: SparkConf,
    tokenManager: HadoopDelegationTokenManager,
    nextRenewal: Long,
    de: RpcEndpointRef) extends Logging {
  private val credentialRenewerThread =
    Executors.newSingleThreadScheduledExecutor(
      ThreadUtils.namedThreadFactory("Credential Refresh Thread"))

  @volatile private var timeOfNextRenewal = nextRenewal

  private val principal = conf.get("spark.yarn.principal")

  private val (secretFile, mode) = getSecretFile(conf)

  private def getSecretFile(conf: SparkConf): (String, String) = {
    val keytab64 = conf.get("spark.yarn.keytab", null)
    val tgt64 = System.getenv("KRB5CCNAME")
    require(keytab64 != null || tgt64 != null, "keytab or tgt required")
    require(keytab64 == null || tgt64 == null, "keytab and tgt cannot be used at the same time")
    val mode = if (keytab64 != null) "keytab" else "tgt"
    val secretFile = if (keytab64 != null) keytab64 else tgt64
    logInfo(s"Logging in as $principal with mode $mode to retrieve HDFS delegation tokens")
    logDebug(s"secretFile is $secretFile")
    (secretFile, mode)
  }

  def scheduleTokenRenewal(): Unit = {
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
            val creds = getRenewedDelegationTokens(conf)
            broadcastDelegationTokens(creds)
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

  private def getRenewedDelegationTokens(conf: SparkConf): Array[Byte] = {
    logInfo(s"Attempting to login with ${conf.get("spark.yarn.principal", null)}")
    // Get new delegation tokens by logging in with a new UGI
    // inspired by AMCredentialRenewer.scala:L174
    val ugi = if (mode == "keytab") {
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, secretFile)
    } else {
      UserGroupInformation.getUGIFromTicketCache(secretFile, principal)
    }
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
      val rt = 0.75 * (nextRenewalTime - currTime)
      (currTime + rt).toLong
    }
    logInfo(s"Time of next renewal is $timeOfNextRenewal")

    // Add the temp credentials back to the original ones.
    UserGroupInformation.getCurrentUser.addCredentials(tempCreds)
    SparkHadoopUtil.get.serialize(tempCreds)
  }

  private def broadcastDelegationTokens(tokens: Array[Byte]): Unit = {
    // send token to existing executors
    logInfo("Sending new tokens to all executors")
    de.send(UpdateDelegationTokens(tokens))
  }
}

object MesosCredentialRenewer extends Logging {
  def getTokenRenewalTime(bytes: Array[Byte], conf: SparkConf): Long = {
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val creds = SparkHadoopUtil.get.deserialize(bytes)
    val renewalTimes = creds.getAllTokens.asScala.flatMap { t =>
      Try {
        t.renew(hadoopConf)
      }.toOption
    }
    if (renewalTimes.isEmpty) Long.MaxValue else renewalTimes.min
  }
}

