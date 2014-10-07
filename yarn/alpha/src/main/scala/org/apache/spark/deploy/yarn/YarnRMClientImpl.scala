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
import java.net.URI;

import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.util.Utils

/**
 * YarnRMClient implementation for the Yarn alpha API.
 */
private class YarnRMClientImpl(args: ApplicationMasterArguments) extends YarnRMClient with Logging {

  private var rpc: YarnRPC = null
  private var resourceManager: AMRMProtocol = _
  private var uiHistoryAddress: String = _
  private var registered: Boolean = false

  override def register(
      conf: YarnConfiguration,
      sparkConf: SparkConf,
      preferredNodeLocations: Map[String, Set[SplitInfo]],
      uiAddress: String,
      uiHistoryAddress: String,
      securityMgr: SecurityManager) = {
    this.rpc = YarnRPC.create(conf)
    this.uiHistoryAddress = uiHistoryAddress

    synchronized {
      resourceManager = registerWithResourceManager(conf)
      registerApplicationMaster(uiAddress)
      registered = true
    }

    new YarnAllocationHandler(conf, sparkConf, resourceManager, getAttemptId(), args,
      preferredNodeLocations, securityMgr)
  }

  override def getAttemptId() = {
    val envs = System.getenv()
    val containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV)
    val containerId = ConverterUtils.toContainerId(containerIdString)
    val appAttemptId = containerId.getApplicationAttemptId()
    appAttemptId
  }

  override def unregister(status: FinalApplicationStatus, diagnostics: String = "") = synchronized {
    if (registered) {
      val finishReq = Records.newRecord(classOf[FinishApplicationMasterRequest])
        .asInstanceOf[FinishApplicationMasterRequest]
      finishReq.setAppAttemptId(getAttemptId())
      finishReq.setFinishApplicationStatus(status)
      finishReq.setDiagnostics(diagnostics)
      finishReq.setTrackingUrl(uiHistoryAddress)
      resourceManager.finishApplicationMaster(finishReq)
    }
  }

  override def getAmIpFilterParams(conf: YarnConfiguration, proxyBase: String) = {
    val proxy = YarnConfiguration.getProxyHostAndPort(conf)
    val parts = proxy.split(":")
    val uriBase = "http://" + proxy + proxyBase
    Map("PROXY_HOST" -> parts(0), "PROXY_URI_BASE" -> uriBase)
  }

  override def getMaxRegAttempts(conf: YarnConfiguration) =
    conf.getInt(YarnConfiguration.RM_AM_MAX_RETRIES, YarnConfiguration.DEFAULT_RM_AM_MAX_RETRIES)

  private def registerWithResourceManager(conf: YarnConfiguration): AMRMProtocol = {
    val rmAddress = NetUtils.createSocketAddr(conf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS))
    logInfo("Connecting to ResourceManager at " + rmAddress)
    rpc.getProxy(classOf[AMRMProtocol], rmAddress, conf).asInstanceOf[AMRMProtocol]
  }

  private def registerApplicationMaster(uiAddress: String): RegisterApplicationMasterResponse = {
    val appMasterRequest = Records.newRecord(classOf[RegisterApplicationMasterRequest])
      .asInstanceOf[RegisterApplicationMasterRequest]
    appMasterRequest.setApplicationAttemptId(getAttemptId())
    // Setting this to master host,port - so that the ApplicationReport at client has some
    // sensible info.
    // Users can then monitor stderr/stdout on that node if required.
    appMasterRequest.setHost(Utils.localHostName())
    appMasterRequest.setRpcPort(0)
    // remove the scheme from the url if it exists since Hadoop does not expect scheme
    appMasterRequest.setTrackingUrl(new URI(uiAddress).getAuthority())
    resourceManager.registerApplicationMaster(appMasterRequest)
  }

}
