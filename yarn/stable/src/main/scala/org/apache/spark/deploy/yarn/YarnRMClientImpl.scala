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

import scala.collection.{Map, Set}

import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.webapp.util.WebAppUtils

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.util.Utils


/**
 * YarnRMClient implementation for the Yarn stable API.
 */
private class YarnRMClientImpl(args: ApplicationMasterArguments) extends YarnRMClient with Logging {

  private var amClient: AMRMClient[ContainerRequest] = _
  private var uiHistoryAddress: String = _

  override def register(
      conf: YarnConfiguration,
      sparkConf: SparkConf,
      preferredNodeLocations: Map[String, Set[SplitInfo]],
      uiAddress: String,
      uiHistoryAddress: String) = {
    amClient = AMRMClient.createAMRMClient()
    amClient.init(conf)
    amClient.start()
    this.uiHistoryAddress = uiHistoryAddress

    logInfo("Registering the ApplicationMaster")
    amClient.registerApplicationMaster(Utils.localHostName(), 0, uiAddress)
    new YarnAllocationHandler(conf, sparkConf, amClient, getAttemptId(), args,
      preferredNodeLocations)
  }

  override def shutdown(status: FinalApplicationStatus, diagnostics: String = "") =
    amClient.unregisterApplicationMaster(status, diagnostics, uiHistoryAddress)

  override def getAttemptId() = {
    val containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name())
    val containerId = ConverterUtils.toContainerId(containerIdString)
    val appAttemptId = containerId.getApplicationAttemptId()
    appAttemptId
  }

  override def getProxyHostAndPort(conf: YarnConfiguration) = WebAppUtils.getProxyHostAndPort(conf)

  override def getMaxRegAttempts(conf: YarnConfiguration) =
    conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)

}
