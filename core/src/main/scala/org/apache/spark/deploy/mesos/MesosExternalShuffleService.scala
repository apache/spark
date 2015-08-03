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

import java.net.SocketAddress
import java.util.concurrent.CountDownLatch

import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage
import org.apache.spark.network.shuffle.protocol.mesos.RegisterDriver
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SecurityManager, SparkConf}

import scala.collection.mutable


/**
 * MesosExternalShuffleServiceEndpoint is a RPC endpoint that receives
 * registration requests from Spark drivers launched with Mesos.
 * It detects driver termination and calls the cleanup callback to [[ExternalShuffleService]]
 */
private[mesos] class MesosExternalShuffleBlockHandler(transportConf: TransportConf)
  extends ExternalShuffleBlockHandler(transportConf) with Logging {

  // Stores a map of driver socket addresses to app ids
  private val connectedApps = new mutable.HashMap[SocketAddress, String]

  protected override def handleMessage(
      message: BlockTransferMessage,
      client: TransportClient,
      callback: RpcResponseCallback): Unit = {
    message match {
      case RegisterDriverParam(appId) =>
        val address = client.getSocketAddress()
        logDebug(s"Received registration request from app $appId, address $address")
        if (connectedApps.contains(address)) {
          val existingAppId: String = connectedApps(address)
          if (!existingAppId.equals(appId)) {
            logError(s"A new app id $appId has connected to existing address $address" +
              s", removing registered app $existingAppId")
            applicationRemoved(existingAppId, true)
          }
        }
        connectedApps(address) = appId
        callback.onSuccess(new Array[Byte](0))
      case _ => super.handleMessage(message, client, callback)
    }
  }

  override def connectionTerminated(client: TransportClient): Unit = {
    val address = client.getSocketAddress()
    if (connectedApps.contains(address)) {
      val appId = connectedApps(address)
      logInfo(s"Application $appId disconnected (address was $address)")
      applicationRemoved(appId, true)
      connectedApps.remove(address)
    } else {
      logWarning(s"Address $address not found in mesos shuffle service")
    }
  }
}

/**
 * An extractor object for matching RegisterDriver message.
 */
private[mesos] object RegisterDriverParam {
  def unapply(r: RegisterDriver): Option[String] = Some(r.getAppId())
}

/**
 * MesosExternalShuffleService wraps [[ExternalShuffleService]] which provides an additional
 * endpoint for drivers to associate with. This allows the shuffle service to detect when
 * a driver is terminated and can further clean up the cached shuffle data.
 */
private[mesos] class MesosExternalShuffleService(
    conf: SparkConf,
    securityManager: SecurityManager,
    transportConf: TransportConf)
  extends ExternalShuffleService(
    conf, securityManager, transportConf, new MesosExternalShuffleBlockHandler(transportConf)) {
}

private[spark] object MesosExternalShuffleService extends Logging {
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
    val transportConf = SparkTransportConf.fromSparkConf(sparkConf, numUsableCores = 0)
    server = new MesosExternalShuffleService(
      sparkConf, securityManager, transportConf)
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


