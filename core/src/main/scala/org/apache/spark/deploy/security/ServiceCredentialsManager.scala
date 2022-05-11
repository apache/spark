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

import java.util.ServiceLoader
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.security.ServiceCredentialsProvider
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.ThreadUtils

/**
 * Common functionality for various types of credentials providing services
 * @param sparkConf Spark configuration
 * @param hadoopConf Hadoop Configuration
 * @param schedulerRef A scheduler ref to allow credentials to be shared between driver and
 *                     executors
 * @tparam T a subtype of ServiceCredentialsProvider. This is need to allow derived classes to
 *           provide their own ServiceLoaders. The ServiceLoader class' load method does not
 *           work with generics and so cannot be used directly in this class
 */
abstract private[spark] class ServiceCredentialsManager[T <: ServiceCredentialsProvider](
    protected val sparkConf: SparkConf,
    protected val hadoopConf: Configuration,
    protected val schedulerRef: RpcEndpointRef) extends Logging {

  def credentialsType: String

  def credentialsConfig: ServiceCredentialsConfig

  def getProviderLoader: ServiceLoader[T]

  /** @return Whether credentials renewal is enabled. */
  def renewalEnabled: Boolean

  def updateCredentialsGrantingTicket(): Unit

  def updateCredentialsTask(): Array[Byte]

  protected var renewalExecutor: ScheduledExecutorService = _

  protected var credentialsProviders: Map[String, T] = loadProviders()
  logDebug(s"Using the following builtin $credentialsType providers: " +
    s"${credentialsProviders.keys.mkString(", ")}.")


  /**
   * Start the credentials renewer.
   *
   * @return New set of credentials
   */
  def start(): Array[Byte] = {
    require(renewalEnabled, s"$credentialsType renewal must be enabled to start the renewer.")
    require(schedulerRef != null, s"$credentialsType renewal requires a scheduler endpoint.")
    renewalExecutor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"$credentialsType Renewal Thread")

    updateCredentialsGrantingTicket()
    updateCredentialsTask()
  }

  def stop(): Unit = {
    if (renewalExecutor != null) {
      renewalExecutor.shutdownNow()
    }
  }

  // Visible for testing.
  def isProviderLoaded(serviceName: String): Boolean = {
    credentialsProviders.contains(serviceName)
  }

  def calculateNextRenewalInterval(nextRenewal: Long): Long = {
    // Calculate the time when new credentials should be created, based on the configured
    // ratio. This ratio is shared by all credentials services and providers
    val now = System.currentTimeMillis
    val ratio = sparkConf.get(CREDENTIALS_RENEWAL_INTERVAL_RATIO)
    val delay = (ratio * (nextRenewal - now)).toLong
    logInfo(s"Calculated delay on renewal is $delay, based on next renewal $nextRenewal " +
      s"and the ratio $ratio, and current time $now")
    delay
  }

  protected def scheduleRenewal(delay: Long): Unit = {
    val _delay = math.max(0, delay)
    logInfo(s"Scheduling renewal in ${UIUtils.formatDuration(_delay)}.")

    val renewalTask = new Runnable() {
      override def run(): Unit = {
        updateCredentialsTask()
      }
    }
    renewalExecutor.schedule(renewalTask, _delay, TimeUnit.MILLISECONDS)
  }

  protected def loadProviders(): Map[String, T] = {
    val loader: ServiceLoader[T] = getProviderLoader
    val providers = mutable.ArrayBuffer[T]()

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
      .filter { p => credentialsConfig.isServiceEnabled(sparkConf, p.serviceName) }
      .map { p => (p.serviceName, p) }
      .toMap
  }
}
