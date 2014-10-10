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

import java.nio.ByteBuffer
import java.security.PrivilegedExceptionAction

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.client.api.NMClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}

import org.apache.spark.{SecurityManager, SparkConf, Logging}


class ExecutorRunnable(
    container: Container,
    conf: Configuration,
    spConf: SparkConf,
    masterAddress: String,
    slaveId: String,
    hostname: String,
    executorMemory: Int,
    executorCores: Int,
    appId: String,
    securityMgr: SecurityManager)
  extends Runnable with ExecutorRunnableUtil with Logging {

  var rpc: YarnRPC = YarnRPC.create(conf)
  var nmClient: NMClient = _
  val sparkConf = spConf
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)

  def run = {
    logInfo("Starting Executor Container")
    nmClient = NMClient.createNMClient()
    nmClient.init(yarnConf)
    nmClient.start()
    startContainer
  }

  def startContainer = {
    logInfo("Setting up ContainerLaunchContext")

    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
      .asInstanceOf[ContainerLaunchContext]

    val localResources = prepareLocalResources
    ctx.setLocalResources(localResources)

    ctx.setEnvironment(env)

    val credentials = UserGroupInformation.getCurrentUser().getCredentials()
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    ctx.setTokens(ByteBuffer.wrap(dob.getData()))

    val commands = prepareCommand(masterAddress, slaveId, hostname, executorMemory, executorCores,
      appId, localResources)

    logInfo(s"Setting up executor with environment: $env")
    logInfo("Setting up executor with commands: " + commands)
    ctx.setCommands(commands)

    ctx.setApplicationACLs(YarnSparkHadoopUtil.getApplicationAclsForYarn(securityMgr))

    // Send the start request to the ContainerManager
    nmClient.startContainer(container, ctx)
  }

}
