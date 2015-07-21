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

package org.apache.spark.deploy.mesos

import java.util.concurrent.CountDownLatch

import scala.collection.mutable

import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.rpc.{RpcAddress, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SecurityManager, SparkConf}

private[mesos] class MesosExternalShuffleService(conf: SparkConf, securityManager: SecurityManager)
  extends ExternalShuffleService(conf, securityManager) {

  private var rpcEnv: RpcEnv = _

  override def start(): Unit = {
    super.start()
    val port = conf.getInt("spark.mesos.shuffle.service.port", 7327)
    logInfo(s"Starting Mesos external shuffle service endpoint on port $port")
    rpcEnv = RpcEnv.create(
      MesosExternalShuffleService.SYSTEM_NAME,
      Utils.localHostName(),
      port,
      conf,
      securityManager)
    val endpoint = new MesosExternalShuffleServiceEndpoint(rpcEnv)
    rpcEnv.setupEndpoint(MesosExternalShuffleService.ENDPOINT_NAME, endpoint)
  }

  override def stop(): Unit = {
    rpcEnv.shutdown()
    rpcEnv.awaitTermination()
    super.stop()
  }

  private class MesosExternalShuffleServiceEndpoint(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {

    // Stores a map of driver rpc addresses to app ids
    private val connectedApps = new mutable.HashMap[RpcAddress, String]

    override def receive: PartialFunction[Any, Unit] = {
      case RegisterMesosDriver(appId, address) =>
        logDebug(s"Received registration request from app $appId, address $address")
        if (connectedApps.contains(address)) {
          val existingAppId: String = connectedApps(address)
          if (!existingAppId.equals(appId)) {
            logError(s"A new app id $appId has connected to existing address $address" +
                     s", removing registered app $existingAppId")
            blockHandler.applicationRemoved(existingAppId, true)
          }
        } else {
          connectedApps(address) = appId
        }
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      logDebug(s"Received disconnect from $remoteAddress")
      if (connectedApps.contains(remoteAddress)) {
        blockHandler.applicationRemoved(connectedApps(remoteAddress), true)
        connectedApps.remove(remoteAddress)
      } else {
        logWarning(s"Address $remoteAddress not found in mesos shuffle service")
      }
    }
  }
}

case class RegisterMesosDriver(appId: String, address: RpcAddress)

object MesosExternalShuffleService extends Logging {
  val SYSTEM_NAME = "mesosExternalShuffleService"
  val ENDPOINT_NAME = "mesosExternalShuffleServiceEndpoint"

  @volatile
  private var server: MesosExternalShuffleService = _

  private val barrier = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    Utils.loadDefaultSparkProperties(sparkConf)
    val securityManager = new SecurityManager(sparkConf)

    // we override this value since this service is started from the command line
    // and we assume the user really wants it to be running
    sparkConf.set("spark.shuffle.service.enabled", "true")
    server = new MesosExternalShuffleService(sparkConf, securityManager)
    server.start()

    installShutdownHook()

    // keep running until the process is terminated
    barrier.await()
  }

  private def installShutdownHook(): Unit = {
    Runtime.getRuntime.addShutdownHook(
      new Thread("Mesos External Shuffle Service shutdown thread") {
      override def run() {
        logInfo("Shutting down Mesos shuffle service.")
        server.stop()
        barrier.countDown()
      }
    })
  }
}


