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

import java.net.URI
import java.nio.ByteBuffer
import java.security.PrivilegedExceptionAction

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records, ProtoUtils}

import org.apache.spark.{SecurityManager, SparkConf, Logging}
import org.apache.spark.network.util.JavaUtils

@deprecated("use yarn/stable", "1.2.0")
class ExecutorRunnable(
    container: Container,
    conf: Configuration,
    spConf: SparkConf,
    masterAddress: String,
    slaveId: String,
    hostname: String,
    executorMemory: Int,
    executorCores: Int,
    appAttemptId: String,
    securityMgr: SecurityManager)
  extends Runnable with ExecutorRunnableUtil with Logging {

  var rpc: YarnRPC = YarnRPC.create(conf)
  var cm: ContainerManager = _
  val sparkConf = spConf
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)

  def run = {
    logInfo("Starting Executor Container")
    cm = connectToCM
    startContainer
  }

  def startContainer = {
    logInfo("Setting up ContainerLaunchContext")

    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
      .asInstanceOf[ContainerLaunchContext]

    ctx.setContainerId(container.getId())
    ctx.setResource(container.getResource())
    val localResources = prepareLocalResources
    ctx.setLocalResources(localResources)

    val env = prepareEnvironment
    ctx.setEnvironment(env)

    ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName())

    val credentials = UserGroupInformation.getCurrentUser().getCredentials()
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    ctx.setContainerTokens(ByteBuffer.wrap(dob.getData()))

    val commands = prepareCommand(masterAddress, slaveId, hostname, executorMemory, executorCores,
      appAttemptId, localResources)
    logInfo("Setting up executor with commands: " + commands)
    ctx.setCommands(commands)

    ctx.setApplicationACLs(YarnSparkHadoopUtil.getApplicationAclsForYarn(securityMgr))

    // If external shuffle service is enabled, register with the Yarn shuffle service already
    // started on the NodeManager and, if authentication is enabled, provide it with our secret
    // key for fetching shuffle files later
    if (sparkConf.getBoolean("spark.shuffle.service.enabled", false)) {
      val secretString = securityMgr.getSecretKey()
      val secretBytes =
        if (secretString != null) {
          // This conversion must match how the YarnShuffleService decodes our secret
          JavaUtils.stringToBytes(secretString)
        } else {
          // Authentication is not enabled, so just provide dummy metadata
          ByteBuffer.allocate(0)
        }
      ctx.setServiceData(Map[String, ByteBuffer]("spark_shuffle" -> secretBytes))
    }

    // Send the start request to the ContainerManager
    val startReq = Records.newRecord(classOf[StartContainerRequest])
    .asInstanceOf[StartContainerRequest]
    startReq.setContainerLaunchContext(ctx)
    cm.startContainer(startReq)
  }

  def connectToCM: ContainerManager = {
    val cmHostPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
    val cmAddress = NetUtils.createSocketAddr(cmHostPortStr)
    logInfo("Connecting to ContainerManager at " + cmHostPortStr)

    // Use doAs and remoteUser here so we can add the container token and not pollute the current
    // users credentials with all of the individual container tokens
    val user = UserGroupInformation.createRemoteUser(container.getId().toString())
    val containerToken = container.getContainerToken()
    if (containerToken != null) {
      user.addToken(ProtoUtils.convertFromProtoFormat(containerToken, cmAddress))
    }

    val proxy = user
        .doAs(new PrivilegedExceptionAction[ContainerManager] {
          def run: ContainerManager = {
            rpc.getProxy(classOf[ContainerManager], cmAddress, conf).asInstanceOf[ContainerManager]
          }
        })
    proxy
  }

}
