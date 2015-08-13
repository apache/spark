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

import scala.collection.mutable

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage
import org.apache.spark.network.shuffle.protocol.mesos.RegisterDriver
import org.apache.spark.network.util.TransportConf

/**
 * An RPC endpoint that receives registration requests from Spark drivers running on Mesos.
 * It detects driver termination and calls the cleanup callback to [[ExternalShuffleService]].
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
        val address = client.getSocketAddress
        logDebug(s"Received registration request from app $appId (remote address $address).")
        if (connectedApps.contains(address)) {
          val existingAppId = connectedApps(address)
          if (!existingAppId.equals(appId)) {
            logError(s"A new app '$appId' has connected to existing address $address, " +
              s"removing previously registered app '$existingAppId'.")
            applicationRemoved(existingAppId, true)
          }
        }
        connectedApps(address) = appId
        callback.onSuccess(new Array[Byte](0))
      case _ => super.handleMessage(message, client, callback)
    }
  }

  /**
   * On connection termination, clean up shuffle files written by the associated application.
   */
  override def connectionTerminated(client: TransportClient): Unit = {
    val address = client.getSocketAddress
    if (connectedApps.contains(address)) {
      val appId = connectedApps(address)
      logInfo(s"Application $appId disconnected (address was $address).")
      applicationRemoved(appId, true /* cleanupLocalDirs */)
      connectedApps.remove(address)
    } else {
      logWarning(s"Unknown $address disconnected.")
    }
  }

  /** An extractor object for matching [[RegisterDriver]] message. */
  private object RegisterDriverParam {
    def unapply(r: RegisterDriver): Option[String] = Some(r.getAppId)
  }
}

/**
 * A wrapper of [[ExternalShuffleService]] that provides an additional endpoint for drivers
 * to associate with. This allows the shuffle service to detect when a driver is terminated
 * and can clean up the associated shuffle files.
 */
private[mesos] class MesosExternalShuffleService(conf: SparkConf, securityManager: SecurityManager)
  extends ExternalShuffleService(conf, securityManager) {

  protected override def newShuffleBlockHandler(
      conf: TransportConf): ExternalShuffleBlockHandler = {
    new MesosExternalShuffleBlockHandler(conf)
  }
}

private[spark] object MesosExternalShuffleService extends Logging {

  def main(args: Array[String]): Unit = {
    ExternalShuffleService.main(args,
      (conf: SparkConf, sm: SecurityManager) => new MesosExternalShuffleService(conf, sm))
  }
}


