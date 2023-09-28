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

package org.apache.spark.deploy.yarn

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.webapp.util.WebAppUtils

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

/**
 * Handles registering and unregistering the application with the YARN ResourceManager.
 */
private[spark] class YarnRMClient extends Logging {

  private var amClient: AMRMClient[ContainerRequest] = _
  private var uiHistoryAddress: String = _
  private var registered: Boolean = false

  /**
   * Registers the application master with the RM.
   *
   * @param driverHost Host name where driver is running.
   * @param driverPort Port where driver is listening.
   * @param conf The Yarn configuration.
   * @param sparkConf The Spark configuration.
   * @param uiAddress Address of the SparkUI.
   * @param uiHistoryAddress Address of the application on the History Server.
   */
  def register(
      driverHost: String,
      driverPort: Int,
      conf: YarnConfiguration,
      sparkConf: SparkConf,
      uiAddress: Option[String],
      uiHistoryAddress: String): Unit = {
    amClient = AMRMClient.createAMRMClient()
    amClient.init(conf)
    amClient.start()
    this.uiHistoryAddress = uiHistoryAddress

    val trackingUrl = uiAddress.getOrElse {
      if (sparkConf.get(ALLOW_HISTORY_SERVER_TRACKING_URL)) uiHistoryAddress else ""
    }

    logInfo("Registering the ApplicationMaster")
    synchronized {
      amClient.registerApplicationMaster(driverHost, driverPort, trackingUrl)
      registered = true
    }
  }

  def createAllocator(
      conf: YarnConfiguration,
      sparkConf: SparkConf,
      appAttemptId: ApplicationAttemptId,
      driverUrl: String,
      driverRef: RpcEndpointRef,
      securityMgr: SecurityManager,
      localResources: Map[String, LocalResource]): YarnAllocator = {
    require(registered, "Must register AM before creating allocator.")
    new YarnAllocator(driverUrl, driverRef, conf, sparkConf, amClient, appAttemptId, securityMgr,
      localResources, SparkRackResolver.get(conf))
  }

  /**
   * Unregister the AM. Guaranteed to only be called once.
   *
   * @param status The final status of the AM.
   * @param diagnostics Diagnostics message to include in the final status.
   */
  def unregister(status: FinalApplicationStatus, diagnostics: String = ""): Unit = synchronized {
    if (registered) {
      amClient.unregisterApplicationMaster(status, diagnostics, uiHistoryAddress)
    }
    if (amClient != null) {
      amClient.stop()
    }
  }

  /** Returns the configuration for the AmIpFilter to add to the Spark UI. */
  def getAmIpFilterParams(conf: YarnConfiguration, proxyBase: String): Map[String, String] = {
    // Figure out which scheme Yarn is using. Note the method seems to have been added after 2.2,
    // so not all stable releases have it.
    val prefix = WebAppUtils.getHttpSchemePrefix(conf)
    val proxies = WebAppUtils.getProxyHostsAndPortsForAmFilter(conf)
    val hosts = proxies.asScala.map(proxy => Utils.parseHostPort(proxy)._1)
    val uriBases = proxies.asScala.map { proxy => prefix + proxy + proxyBase }
    val params =
      Map("PROXY_HOSTS" -> hosts.mkString(","), "PROXY_URI_BASES" -> uriBases.mkString(","))

    // Handles RM HA urls
    val rmIds = conf.getStringCollection(YarnConfiguration.RM_HA_IDS).asScala
    if (rmIds != null && rmIds.nonEmpty) {
      params + ("RM_HA_URLS" -> rmIds.map(getUrlByRmId(conf, _)).mkString(","))
    } else {
      params
    }
  }

  /** Returns the maximum number of attempts to register the AM. */
  def getMaxRegAttempts(sparkConf: SparkConf, yarnConf: YarnConfiguration): Int = {
    val sparkMaxAttempts = sparkConf.get(MAX_APP_ATTEMPTS)
    val yarnMaxAttempts = yarnConf.getInt(
      YarnConfiguration.RM_AM_MAX_ATTEMPTS, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
    sparkMaxAttempts match {
      case Some(x) => if (x <= yarnMaxAttempts) x else yarnMaxAttempts
      case None => yarnMaxAttempts
    }
  }

  private def getUrlByRmId(conf: Configuration, rmId: String): String = {
    val addressPropertyPrefix = if (YarnConfiguration.useHttps(conf)) {
      YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS
    } else {
      YarnConfiguration.RM_WEBAPP_ADDRESS
    }

    val addressWithRmId = if (rmId == null || rmId.isEmpty) {
      addressPropertyPrefix
    } else if (rmId.startsWith(".")) {
      throw new IllegalStateException(s"rmId $rmId should not already have '.' prepended.")
    } else {
      s"$addressPropertyPrefix.$rmId"
    }

    conf.get(addressWithRmId)
  }
}
