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
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.{ServiceCredentialsConfig, ServiceCredentialsManager}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateCloudCredentials
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils


/**
 * Manager for cloud credentials in a Spark application.
 * The manager will obtain credentials from all of the enabled cloud credentials providers,
 * distribute the credentials to the driver and executor, and also start a renewal thread to
 * refresh credentials if they are about to expire.
 * Credentials distributed to the driver/executors are accessible in the hadoop configuration.
 */
private[spark] class CloudCredentialsManager(
    override protected val sparkConf: SparkConf,
    override protected val hadoopConf: Configuration,
    override protected val schedulerRef: RpcEndpointRef)
  extends ServiceCredentialsManager[CloudCredentialsProvider](sparkConf, hadoopConf, schedulerRef) {

  val cloudCredentialsProviders: Map[String, CloudCredentialsProvider] = credentialsProviders
      .asInstanceOf[Map[String, CloudCredentialsProvider]]

  def credentialsType: String = "Cloud service credentials"

  def credentialsConfig: ServiceCredentialsConfig = CloudCredentialsManager

  def getProviderLoader: ServiceLoader[CloudCredentialsProvider] =
    ServiceLoader.load(classOf[CloudCredentialsProvider], Utils.getContextOrSparkClassLoader)

  def renewalEnabled: Boolean = true

  /**
   * Start the credentials renewer. Upon start, the renewer will
   * obtain credentials for all configured services and send them to the driver, and
   * set up tasks to renew credentials before they expire
   *
   * @return New set of credentials for the service.
   */
  override def start(): Array[Byte] = {
    super.start()
  }

  def updateCredentialsGrantingTicket(): Unit = {}

  def updateCredentialsTask(): Array[Byte] = {
    cloudCredentialsProviders
      .values
      .filter(p => CloudCredentialsManager.isServiceEnabled(sparkConf, p.serviceName))
      .foreach(p => updateServiceCredentialsTask(p.serviceName))
    // Return an empty byte array only to satisfy the compiler
    Array.emptyByteArray
  }

  /**
   * Fetch new credentials for a configured credentials service.
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

  private def scheduleRenewal(serviceName: String, delay: Long): Unit = {
    val _delay = math.max(0, delay)
    logInfo(s"Scheduling $serviceName credentials renewal in ${UIUtils.formatDuration(_delay)}.")

    val renewalTask = new Runnable() {
      override def run(): Unit = {
        updateServiceCredentialsTask(serviceName)
      }
    }
    renewalExecutor.schedule(renewalTask, _delay, TimeUnit.MILLISECONDS)
  }

  /**
   * Periodic task to update credentials.
   */
  private def updateServiceCredentialsTask(serviceName: String): Unit = {
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

    if (creds.expiry.isDefined) {
      val nextRenewal = creds.expiry.get
      val delay = calculateNextRenewalInterval(nextRenewal)

      scheduleRenewal(serviceName, delay)
    }
    creds
  }

}

// This is public so that implementations can access the config variables defined here
object CloudCredentialsManager extends ServiceCredentialsConfig {

  val cloudCredentialsConfig = "spark.security.cloud.credentials.%s"

  override def providerEnabledConfig: String = "spark.security.cloud.credentials.%s.enabled"

  override def deprecatedProviderEnabledConfigs: List[String] = List()

  // Cloud credentials loading is disabled by default.
  override def isServiceEnabled(sparkConf: SparkConf, serviceName: String): Boolean = {
    val key = providerEnabledConfig.format(serviceName)
    sparkConf.getOption(key).exists(_.toBoolean)
  }
}
