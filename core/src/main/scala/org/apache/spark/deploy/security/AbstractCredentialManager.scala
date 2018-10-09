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
package org.apache.spark.deploy.security

import java.io.File
import java.security.PrivilegedExceptionAction
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.ThreadUtils

/**
 * Base class for managing delegation tokens for a Spark application.
 *
 * This manager has two modes of operation:
 *
 * 1.  When configured with a principal and a keytab, it will make sure long-running apps can run
 * without interruption while accessing secured services. It periodically logs in to the KDC with
 * user-provided credentials, and contacts all the configured secure services to obtain delegation
 * tokens to be distributed to the rest of the application.
 *
 * Because the Hadoop UGI API does not expose the TTL of the TGT, a configuration controls how often
 * to check that a relogin is necessary. This is done reasonably often since the check is a no-op
 * when the relogin is not yet needed. The check period can be overridden in the configuration.
 *
 * New delegation tokens are created once 75% of the renewal interval of the original tokens has
 * elapsed. The new tokens are sent to the Spark driver endpoint once it's registered with the AM.
 * The driver is tasked with distributing the tokens to other processes that might need them.
 *
 * 2. When operating without an explicit principal and keytab, token renewal will not be available.
 * Starting the manager will distribute an initial set of delegation tokens to the provided Spark
 * driver, but the app will not get new tokens when those expire.
 */
private[spark] abstract class AbstractCredentialManager(
    protected val sparkConf: SparkConf,
    protected val hadoopConf: Configuration) extends Logging {

  private val principal = sparkConf.get(PRINCIPAL).orNull
  private val keytab = sparkConf.get(KEYTAB).orNull

  if (principal != null) {
    require(keytab != null, "Kerberos principal specified without a keytab.")
    require(new File(keytab).isFile(), s"Cannot find keytab at $keytab.")
  }

  private val renewalExecutor: ScheduledExecutorService =
    if (principal != null) {
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("Credential Renewal Thread")
    } else {
      null
    }

  private val driverRef = new AtomicReference[RpcEndpointRef]()

  protected def setDriverRef(ref: RpcEndpointRef): Unit = {
    driverRef.set(ref)
  }

  /**
   * Start the token renewer. Upon start, if a principal has been configured, the renewer will:
   *
   * - log in the configured principal, and set up a task to keep that user's ticket renewed
   * - obtain delegation tokens from all available providers
   * - schedule a periodic task to update the tokens when needed.
   *
   * When token renewal is not enabled, this method will not start any periodic tasks. Instead, it
   * will generate tokens if the driver ref has been provided, update the current user, and send
   * those tokens to the driver. No future tokens will be generated, so when that initial set
   * expires, the app will stop working.
   *
   * @param driver If provided, the driver where to send the newly generated tokens.
   *               The same ref will also receive future token updates unless overridden later.
   * @return The newly logged in user, or null if principal is not configured.
   */
  def start(driver: Option[RpcEndpointRef] = None): UserGroupInformation = {
    driver.foreach(setDriverRef)

    if (principal != null) {
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

      val driver = driverRef.get()
      if (driver != null) {
        val tokens = SparkHadoopUtil.get.serialize(creds)
        driver.send(UpdateDelegationTokens(tokens))
      }

      // Transfer the original user's tokens to the new user, since it may contain needed tokens
      // (such as those user to connect to YARN). Explicitly avoid overwriting tokens that already
      // exist in the current user's credentials, since those were freshly obtained above
      // (see SPARK-23361).
      val existing = ugi.getCredentials()
      existing.mergeAll(originalCreds)
      ugi.addCredentials(existing)
      ugi
    } else {
      driver.foreach { ref =>
        logInfo("Using ticket cache for Kerberos authentication, no token renewal.")
        val creds = new Credentials()
        obtainDelegationTokens(creds)
        UserGroupInformation.getCurrentUser.addCredentials(creds)

        val tokens = SparkHadoopUtil.get.serialize(creds)
        ref.send(UpdateDelegationTokens(tokens))
      }
      null
    }
  }

  def stop(): Unit = {
    if (renewalExecutor != null) {
      renewalExecutor.shutdown()
    }
  }

  /**
   * Fetch new delegation tokens for configured services, storing them in the given credentials.
   *
   * @return The time by which the tokens must be renewed.
   */
  protected def obtainDelegationTokens(creds: Credentials): Long

  private def scheduleRenewal(delay: Long): Unit = {
    val _delay = math.max(0, delay)
    logInfo(s"Scheduling login from keytab in ${UIUtils.formatDuration(delay)}.")

    val renewalTask = new Runnable() {
      override def run(): Unit = {
        updateTokensTask()
      }
    }
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
        // This shouldn't really happen, since the driver should register way before tokens expire.
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
        val nextRenewal = obtainDelegationTokens(creds)

        // Calculate the time when new credentials should be created, based on the configured
        // ratio.
        val now = System.currentTimeMillis
        val ratio = sparkConf.get(CREDENTIALS_RENEWAL_INTERVAL_RATIO)
        val adjustedNextRenewal = (now + (ratio * (nextRenewal - now))).toLong

        scheduleRenewal(adjustedNextRenewal - now)
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
