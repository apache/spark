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
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.ThreadUtils

/**
 * A manager tasked with periodically updating delegation tokens needed by the application.
 *
 * This manager is meant to make sure long-running apps (such as Spark Streaming apps) can run
 * without interruption while accessing secured services. It periodically logs in to the KDC with
 * user-provided credentials, and contacts all the configured secure services to obtain delegation
 * tokens to be distributed to the rest of the application.
 *
 * This class will manage the kerberos login, by renewing the TGT when needed. Because the UGI API
 * does not expose the TTL of the TGT, a configuration controls how often to check that a relogin is
 * necessary. This is done reasonably often since the check is a no-op when the relogin is not yet
 * needed. The check period can be overridden in the configuration.
 *
 * New delegation tokens are created once 75% of the renewal interval of the original tokens has
 * elapsed. The new tokens are sent to the Spark driver endpoint once it's registered with the AM.
 * The driver is tasked with distributing the tokens to other processes that might need them.
 */
private[yarn] class AMCredentialRenewer(
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends Logging {

  private val principal = sparkConf.get(PRINCIPAL).get
  private val keytab = sparkConf.get(KEYTAB).get
  private val credentialManager = new YARNHadoopDelegationTokenManager(sparkConf, hadoopConf)

  private val renewalExecutor: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("Credential Refresh Thread")

  private val driverRef = new AtomicReference[RpcEndpointRef]()

  private val renewalTask = new Runnable() {
    override def run(): Unit = {
      updateTokensTask()
    }
  }

  def setDriverRef(ref: RpcEndpointRef): Unit = {
    driverRef.set(ref)
  }

  /**
   * Start the token renewer. Upon start, the renewer will:
   *
   * - log in the configured user, and set up a task to keep that user's ticket renewed
   * - obtain delegation tokens from all available providers
   * - schedule a periodic task to update the tokens when needed.
   *
   * @return The newly logged in user.
   */
  def start(): UserGroupInformation = {
    val originalCreds = UserGroupInformation.getCurrentUser().getCredentials()
    val ugi = doLogin()

    val tgtRenewalTask = new Runnable() {
      override def run(): Unit = {
        ugi.checkTGTAndReloginFromKeytab()
      }
    }
    val tgtRenewalPeriod = sparkConf.get(KERBEROS_RELOGIN_PERIOD)
    renewalExecutor.scheduleAtFixedRate(tgtRenewalTask, tgtRenewalPeriod, tgtRenewalPeriod,
      TimeUnit.SECONDS)

    val creds = obtainTokensAndScheduleRenewal(ugi)
    ugi.addCredentials(creds)

    // Transfer the original user's tokens to the new user, since that's needed to connect to
    // YARN. Explicitly avoid overwriting tokens that already exist in the current user's
    // credentials, since those were freshly obtained above (see SPARK-23361).
    val existing = ugi.getCredentials()
    existing.mergeAll(originalCreds)
    ugi.addCredentials(existing)

    ugi
  }

  def stop(): Unit = {
    renewalExecutor.shutdown()
  }

  private def scheduleRenewal(delay: Long): Unit = {
    val _delay = math.max(0, delay)
    logInfo(s"Scheduling login from keytab in ${UIUtils.formatDuration(delay)}.")
    renewalExecutor.schedule(renewalTask, _delay, TimeUnit.MILLISECONDS)
  }

  /**
   * Periodic task to login to the KDC and create new delegation tokens. Re-schedules itself
   * to fetch the next set of tokens when needed.
   */
  private def updateTokensTask(): Unit = {
    try {
      val freshUGI = doLogin()
      val creds = obtainTokensAndScheduleRenewal(freshUGI)
      val tokens = SparkHadoopUtil.get.serialize(creds)

      val driver = driverRef.get()
      if (driver != null) {
        logInfo("Updating delegation tokens.")
        driver.send(UpdateDelegationTokens(tokens))
      } else {
        // This shouldn't really happen, since the driver should register way before tokens expire
        // (or the AM should time out the application).
        logWarning("Delegation tokens close to expiration but no driver has registered yet.")
        SparkHadoopUtil.get.addDelegationTokens(tokens, sparkConf)
      }
    } catch {
      case e: Exception =>
        val delay = TimeUnit.SECONDS.toMillis(sparkConf.get(CREDENTIALS_RENEWAL_RETRY_WAIT))
        logWarning(s"Failed to update tokens, will try again in ${UIUtils.formatDuration(delay)}!" +
          " If this happens too often tasks will fail.", e)
        scheduleRenewal(delay)
    }
  }

  /**
   * Obtain new delegation tokens from the available providers. Schedules a new task to fetch
   * new tokens before the new set expires.
   *
   * @return Credentials containing the new tokens.
   */
  private def obtainTokensAndScheduleRenewal(ugi: UserGroupInformation): Credentials = {
    ugi.doAs(new PrivilegedExceptionAction[Credentials]() {
      override def run(): Credentials = {
        val creds = new Credentials()
        val nextRenewal = credentialManager.obtainDelegationTokens(hadoopConf, creds)

        val timeToWait = SparkHadoopUtil.nextCredentialRenewalTime(nextRenewal, sparkConf) -
          System.currentTimeMillis()
        scheduleRenewal(timeToWait)
        creds
      }
    })
  }

  private def doLogin(): UserGroupInformation = {
    logInfo(s"Attempting to login to KDC using principal: $principal")
    val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
    logInfo("Successfully logged into KDC.")
    ugi
  }

}
