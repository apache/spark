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
import java.net.URI
import java.security.PrivilegedExceptionAction
import java.util.ServiceLoader
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Manager for delegation tokens in a Spark application.
 *
 * When delegation token renewal is enabled, this manager will make sure long-running apps can
 * run without interruption while accessing secured services. It periodically logs in to the KDC
 * with user-provided credentials, and contacts all the configured secure services to obtain
 * delegation tokens to be distributed to the rest of the application.
 *
 * New delegation tokens are created once 75% of the renewal interval of the original tokens has
 * elapsed. The new tokens are sent to the Spark driver endpoint. The driver is tasked with
 * distributing the tokens to other processes that might need them.
 *
 * Renewal can be enabled in two different ways: by providing a principal and keytab to Spark, or by
 * enabling renewal based on the local credential cache. The latter has the drawback that Spark
 * can't create new TGTs by itself, so the user has to manually update the Kerberos ticket cache
 * externally.
 *
 * This class can also be used just to create delegation tokens, by calling the
 * `obtainDelegationTokens` method. This option does not require calling the `start` method nor
 * providing a driver reference, but leaves it up to the caller to distribute the tokens that were
 * generated.
 */
private[spark] class HadoopDelegationTokenManager(
    protected val sparkConf: SparkConf,
    protected val hadoopConf: Configuration,
    protected val schedulerRef: RpcEndpointRef) extends Logging {

  private val principal = sparkConf.get(PRINCIPAL).orNull

  // The keytab can be a local: URI for cluster mode, so translate it to a regular path. If it is
  // needed later on, the code will check that it exists.
  private val keytab = sparkConf.get(KEYTAB).map { uri => new URI(uri).getPath() }.orNull

  require((principal == null) == (keytab == null),
    "Both principal and keytab must be defined, or neither.")

  private val delegationTokenProviders = loadProviders()
  logDebug("Using the following builtin delegation token providers: " +
    s"${delegationTokenProviders.keys.mkString(", ")}.")

  private var renewalExecutor: ScheduledExecutorService = _

  /** @return Whether delegation token renewal is enabled. */
  def renewalEnabled: Boolean = sparkConf.get(KERBEROS_RENEWAL_CREDENTIALS) match {
    case "keytab" => principal != null
    case "ccache" => UserGroupInformation.getCurrentUser().hasKerberosCredentials()
    case _ => false
  }

  /**
   * Start the token renewer. Requires a principal and keytab. Upon start, the renewer will
   * obtain delegation tokens for all configured services and send them to the driver, and
   * set up tasks to periodically get fresh tokens as needed.
   *
   * This method requires that a keytab has been provided to Spark, and will try to keep the
   * logged in user's TGT valid while this manager is active.
   *
   * @return New set of delegation tokens created for the configured principal.
   */
  def start(): Array[Byte] = {
    require(renewalEnabled, "Token renewal must be enabled to start the renewer.")
    require(schedulerRef != null, "Token renewal requires a scheduler endpoint.")
    renewalExecutor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("Credential Renewal Thread")

    val ugi = UserGroupInformation.getCurrentUser()
    if (ugi.isFromKeytab()) {
      // In Hadoop 2.x, renewal of the keytab-based login seems to be automatic, but in Hadoop 3.x,
      // it is configurable (see hadoop.kerberos.keytab.login.autorenewal.enabled, added in
      // HADOOP-9567). This task will make sure that the user stays logged in regardless of that
      // configuration's value. Note that checkTGTAndReloginFromKeytab() is a no-op if the TGT does
      // not need to be renewed yet.
      val tgtRenewalTask = new Runnable() {
        override def run(): Unit = {
          ugi.checkTGTAndReloginFromKeytab()
        }
      }
      val tgtRenewalPeriod = sparkConf.get(KERBEROS_RELOGIN_PERIOD)
      renewalExecutor.scheduleAtFixedRate(tgtRenewalTask, tgtRenewalPeriod, tgtRenewalPeriod,
        TimeUnit.SECONDS)
    }

    updateTokensTask()
  }

  def stop(): Unit = {
    if (renewalExecutor != null) {
      renewalExecutor.shutdownNow()
    }
  }

  /**
   * Fetch new delegation tokens for configured services, storing them in the given credentials.
   *
   * @param creds Credentials object where to store the delegation tokens.
   */
  def obtainDelegationTokens(creds: Credentials): Unit = {
    val currentUser = UserGroupInformation.getCurrentUser()
    val hasKerberosCreds = principal != null ||
      Option(currentUser.getRealUser()).getOrElse(currentUser).hasKerberosCredentials()

    // Delegation tokens can only be obtained if the real user has Kerberos credentials, so
    // skip creation when those are not available.
    if (hasKerberosCreds) {
      val freshUGI = doLogin()
      freshUGI.doAs(new PrivilegedExceptionAction[Unit]() {
        override def run(): Unit = {
          val (newTokens, _) = obtainDelegationTokens()
          creds.addAll(newTokens)
        }
      })
      if (!currentUser.equals(freshUGI)) {
        FileSystem.closeAllForUGI(freshUGI)
      }
    }
  }

  /**
   * Fetch new delegation tokens for configured services.
   *
   * @return 2-tuple (credentials with new tokens, time by which the tokens must be renewed)
   */
  private def obtainDelegationTokens(): (Credentials, Long) = {
    val creds = new Credentials()
    val nextRenewal = delegationTokenProviders.values.flatMap { provider =>
      if (provider.delegationTokensRequired(sparkConf, hadoopConf)) {
        provider.obtainDelegationTokens(hadoopConf, sparkConf, creds)
      } else {
        logDebug(s"Service ${provider.serviceName} does not require a token." +
          s" Check your configuration to see if security is disabled or not.")
        None
      }
    }.foldLeft(Long.MaxValue)(math.min)
    (creds, nextRenewal)
  }

  // Visible for testing.
  def isProviderLoaded(serviceName: String): Boolean = {
    delegationTokenProviders.contains(serviceName)
  }

  private def scheduleRenewal(delay: Long): Unit = {
    val _delay = math.max(0, delay)
    logInfo(log"Scheduling renewal in ${MDC(LogKeys.TIME_UNITS, UIUtils.formatDuration(_delay))}.")

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
  private def updateTokensTask(): Array[Byte] = {
    try {
      val freshUGI = doLogin()
      val creds = obtainTokensAndScheduleRenewal(freshUGI)
      val tokens = SparkHadoopUtil.get.serialize(creds)

      logInfo("Updating delegation tokens.")
      schedulerRef.send(UpdateDelegationTokens(tokens))
      tokens
    } catch {
      case _: InterruptedException =>
        // Ignore, may happen if shutting down.
        null
      case e: Exception =>
        val delay = TimeUnit.SECONDS.toMillis(sparkConf.get(CREDENTIALS_RENEWAL_RETRY_WAIT))
        logWarning(log"Failed to update tokens, will try again in " +
          log"${MDC(LogKeys.TIME_UNITS, UIUtils.formatDuration(delay))}!" +
          log" If this happens too often tasks will fail.", e)
        scheduleRenewal(delay)
        null
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
        val (creds, nextRenewal) = obtainDelegationTokens()

        // Calculate the time when new credentials should be created, based on the configured
        // ratio.
        val now = System.currentTimeMillis
        val ratio = sparkConf.get(CREDENTIALS_RENEWAL_INTERVAL_RATIO)
        val delay = (ratio * (nextRenewal - now)).toLong
        logInfo(log"Calculated delay on renewal is ${MDC(LogKeys.DELAY, delay)}," +
          log" based on next renewal ${MDC(LogKeys.NEXT_RENEWAL_TIME, nextRenewal)}" +
          log" and the ratio ${MDC(LogKeys.CREDENTIALS_RENEWAL_INTERVAL_RATIO, ratio)}," +
          log" and current time ${MDC(LogKeys.CURRENT_TIME, now)}")
        scheduleRenewal(delay)
        creds
      }
    })
  }

  private def doLogin(): UserGroupInformation = {
    if (principal != null) {
      logInfo(log"Attempting to login to KDC using principal: ${MDC(LogKeys.PRINCIPAL, principal)}")
      require(new File(keytab).isFile(), s"Cannot find keytab at $keytab.")
      val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
      logInfo("Successfully logged into KDC.")
      ugi
    } else if (!SparkHadoopUtil.get.isProxyUser(UserGroupInformation.getCurrentUser())) {
      logInfo("Attempting to load user's ticket cache.")
      val ccache = sparkConf.getenv("KRB5CCNAME")
      val user = Option(sparkConf.getenv("KRB5PRINCIPAL")).getOrElse(
        UserGroupInformation.getCurrentUser().getUserName())
      UserGroupInformation.getUGIFromTicketCache(ccache, user)
    } else {
      UserGroupInformation.getCurrentUser()
    }
  }

  private def loadProviders(): Map[String, HadoopDelegationTokenProvider] = {
    val loader = ServiceLoader.load(classOf[HadoopDelegationTokenProvider],
      Utils.getContextOrSparkClassLoader)
    val providers = mutable.ArrayBuffer[HadoopDelegationTokenProvider]()

    val iterator = loader.iterator
    while (iterator.hasNext) {
      try {
        providers += iterator.next
      } catch {
        case t: Throwable =>
          logDebug(s"Failed to load built in provider.", t)
      }
    }

    // Filter out providers for which spark.security.credentials.{service}.enabled is false.
    providers
      .filter { p => HadoopDelegationTokenManager.isServiceEnabled(sparkConf, p.serviceName) }
      .map { p => (p.serviceName, p) }
      .toMap
  }
}

private[spark] object HadoopDelegationTokenManager extends Logging {
  private val providerEnabledConfig = "spark.security.credentials.%s.enabled"

  private val deprecatedProviderEnabledConfigs = List(
    "spark.yarn.security.tokens.%s.enabled",
    "spark.yarn.security.credentials.%s.enabled")

  def isServiceEnabled(sparkConf: SparkConf, serviceName: String): Boolean = {
    val key = providerEnabledConfig.format(serviceName)

    deprecatedProviderEnabledConfigs.foreach { pattern =>
      val deprecatedKey = pattern.format(serviceName)
      if (sparkConf.contains(deprecatedKey)) {
        logWarning(log"${MDC(LogKeys.DEPRECATED_KEY, deprecatedKey)} is deprecated. " +
          log"Please use ${MDC(LogKeys.CONFIG, key)} instead.")
      }
    }

    val isEnabledDeprecated = deprecatedProviderEnabledConfigs.forall { pattern =>
      sparkConf
        .getOption(pattern.format(serviceName))
        .map(_.toBoolean)
        .getOrElse(true)
    }

    sparkConf
      .getOption(key)
      .map(_.toBoolean)
      .getOrElse(isEnabledDeprecated)
  }
}
