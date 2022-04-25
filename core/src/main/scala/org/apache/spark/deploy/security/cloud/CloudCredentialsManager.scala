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

package org.apache.spark.deploy.security.cloud

import java.util.ServiceLoader
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateCloudCredentials
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Manager for cloud credentials in a Spark application.
 * The manager will obtain credentials from all of the enabled cloud credentials providers,
 * distribute the credentials to the driver and executor, and also start a renewal thread to
 * refresh credentials if they are about to expire.
 * Credentials distributed to the driver/executors are accessible in the hadoop configuration.
 */
private[spark] class CloudCredentialsManager(
    protected val sparkConf: SparkConf,
    protected val hadoopConf: Configuration,
    protected val schedulerRef: RpcEndpointRef) extends Logging {

  private val cloudCredentialsProviders = loadProviders()
  logDebug("Using the following cloud credentials providers: " +
    s"${cloudCredentialsProviders.keys.mkString(", ")}.")

  private var renewalExecutor: ScheduledExecutorService = _

  /**
   * Start the credentials renewer. Upon start, the renewer will
   * obtain credentials for all configured services and send them to the driver, and
   * set up tasks to renew credentials before they expire
   * @return New set of credentials for the service.
   */
 def start(): Unit = {
    require(schedulerRef != null, "Credentials renewal requires a scheduler endpoint.")
    renewalExecutor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("Cloud Credentials Renewal Thread")

   cloudCredentialsProviders
     .values
     .filter(p => CloudCredentialsManager.isServiceEnabled(sparkConf, p.serviceName))
     .foreach(p => updateCredentialsTask(p.serviceName))
   ()
  }

  def stop(): Unit = {
    if (renewalExecutor != null) {
      renewalExecutor.shutdownNow()
      renewalExecutor = null
    }
  }

  /**
   * Fetch new credentials for a configured service.
   *
   * @return credentials for the service
   */
  private def obtainCredentials(serviceName: String): CloudCredentials = {
    val creds = cloudCredentialsProviders(serviceName).obtainCredentials(hadoopConf, sparkConf)
    creds match {
      case c if c.isDefined => c.get
      case _ => throw new RuntimeException(s"Failed to get credentials for $serviceName")
    }
  }

  // Visible for testing.
  def isProviderLoaded(serviceName: String): Boolean = {
    cloudCredentialsProviders.contains(serviceName)
  }

  private def scheduleRenewal(serviceName: String, delay: Long): Unit = {
    val _delay = math.max(0, delay)
    logInfo(s"Scheduling renewal in ${UIUtils.formatDuration(_delay)}.")

    val renewalTask = new Runnable() {
      override def run(): Unit = {
        updateCredentialsTask(serviceName)
      }
    }
    renewalExecutor.schedule(renewalTask, _delay, TimeUnit.MILLISECONDS)
  }

  /**
   * Periodic task to update credentials.
   */
  private def updateCredentialsTask(serviceName: String): Unit = {
    try {
      val creds = obtainCredentialsAndScheduleRenewal(serviceName)
      logInfo(s"Updating credentials from ${serviceName}.")
      schedulerRef.send(
        UpdateCloudCredentials(SparkHadoopUtil.get.serializeCloudCredentials(creds)))
    } catch {
      case _: InterruptedException =>
        // Ignore, may happen if shutting down.
    }
  }

  /**
   * Obtain new credentials from the available providers. Schedules a new task to fetch
   * new credentials before the new set expires.
   *
   * @return Credentials containing the new credentials and expiry time.
   */
  private def obtainCredentialsAndScheduleRenewal(serviceName: String): CloudCredentials = {
    val creds = obtainCredentials(serviceName)

    // Calculate the time when new credentials should be created, based on the configured
    // ratio.
    if (creds.expiry.isDefined) {
      val nextRenewal = creds.expiry.get
      val now = System.currentTimeMillis
      val ratio = sparkConf.get(CREDENTIALS_RENEWAL_INTERVAL_RATIO)
      val delay = (ratio * (nextRenewal - now)).toLong
      logInfo(s"Calculated delay on renewal is $delay, based on next renewal $nextRenewal " +
        s"and the ratio $ratio, and current time $now")
      scheduleRenewal(serviceName, delay)
    }
    creds
  }

  private def loadProviders(): Map[String, CloudCredentialsProvider] = {
    val loader = ServiceLoader.load(classOf[CloudCredentialsProvider],
      Utils.getContextOrSparkClassLoader)
    val providers = mutable.ArrayBuffer[CloudCredentialsProvider]()

    val iterator = loader.iterator
    while (iterator.hasNext) {
      try {
        providers += iterator.next
      } catch {
        case t: Throwable =>
          logDebug(s"Failed to load built in provider.", t)
      }
    }
    // Filter out providers for which spark.security.cloud.credentials.{service}.enabled is false.
    providers
      .filter { p => CloudCredentialsManager.isServiceEnabled(sparkConf, p.serviceName) }
      .map { p => (p.serviceName, p) }
      .toMap
  }

}

// This is public so that implementations can access the config variables defined here
object CloudCredentialsManager extends Logging {
  private val providerEnabledConfig = "spark.security.cloud.credentials.%s.enabled"
  val cloudCredentialsConfig = "spark.security.cloud.credentials.%s"

  def isServiceEnabled(sparkConf: SparkConf, serviceName: String): Boolean = {
    val key = providerEnabledConfig.format(serviceName)
    sparkConf.getOption(key).exists(_.toBoolean)
  }
}
