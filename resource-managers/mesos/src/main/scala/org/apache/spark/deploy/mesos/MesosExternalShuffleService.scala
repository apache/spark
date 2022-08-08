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

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.deploy.mesos.config._
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.shuffle.ExternalBlockHandler
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage
import org.apache.spark.network.shuffle.protocol.mesos.{RegisterDriver, ShuffleServiceHeartbeat}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.ThreadUtils

/**
 * An RPC endpoint that receives registration requests from Spark drivers running on Mesos.
 * It detects driver termination and calls the cleanup callback to [[ExternalShuffleService]].
 */
private[mesos] class MesosExternalBlockHandler(
    transportConf: TransportConf,
    cleanerIntervalS: Long)
  extends ExternalBlockHandler(transportConf, null) with Logging {

  ThreadUtils.newDaemonSingleThreadScheduledExecutor("shuffle-cleaner-watcher")
    .scheduleAtFixedRate(new CleanerThread(), 0, cleanerIntervalS, TimeUnit.SECONDS)

  // Stores a map of app id to app state (timeout value and last heartbeat)
  private val connectedApps = new ConcurrentHashMap[String, AppState]()

  protected override def handleMessage(
      message: BlockTransferMessage,
      client: TransportClient,
      callback: RpcResponseCallback): Unit = {
    message match {
      case RegisterDriverParam(appId, appState) =>
        val address = client.getSocketAddress
        val timeout = appState.heartbeatTimeout
        logInfo(s"Received registration request from app $appId (remote address $address, " +
          s"heartbeat timeout $timeout ms).")
        if (connectedApps.containsKey(appId)) {
          logWarning(s"Received a registration request from app $appId, but it was already " +
            s"registered")
        }
        connectedApps.put(appId, appState)
        callback.onSuccess(ByteBuffer.allocate(0))
      case Heartbeat(appId) =>
        val address = client.getSocketAddress
        Option(connectedApps.get(appId)) match {
          case Some(existingAppState) =>
            logTrace(s"Received ShuffleServiceHeartbeat from app '$appId' (remote " +
              s"address $address).")
            existingAppState.lastHeartbeat = System.nanoTime()
          case None =>
            logWarning(s"Received ShuffleServiceHeartbeat from an unknown app (remote " +
              s"address $address, appId '$appId').")
        }
      case _ => super.handleMessage(message, client, callback)
    }
  }

  /** An extractor object for matching [[RegisterDriver]] message. */
  private object RegisterDriverParam {
    def unapply(r: RegisterDriver): Option[(String, AppState)] =
      Some((r.getAppId, new AppState(r.getHeartbeatTimeoutMs, System.nanoTime())))
  }

  private object Heartbeat {
    def unapply(h: ShuffleServiceHeartbeat): Option[String] = Some(h.getAppId)
  }

  private class AppState(val heartbeatTimeout: Long, @volatile var lastHeartbeat: Long)

  private class CleanerThread extends Runnable {
    override def run(): Unit = {
      val now = System.nanoTime()
      connectedApps.asScala.foreach { case (appId, appState) =>
        if (now - appState.lastHeartbeat > appState.heartbeatTimeout * 1000 * 1000) {
          logInfo(s"Application $appId timed out. Removing shuffle files.")
          connectedApps.remove(appId)
          applicationRemoved(appId, true)
        }
      }
    }
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
      conf: TransportConf): ExternalBlockHandler = {
    val cleanerIntervalS = this.conf.get(SHUFFLE_CLEANER_INTERVAL_S)
    new MesosExternalBlockHandler(conf, cleanerIntervalS)
  }
}

private[spark] object MesosExternalShuffleService extends Logging {

  def main(args: Array[String]): Unit = {
    ExternalShuffleService.main(args,
      (conf: SparkConf, sm: SecurityManager) => new MesosExternalShuffleService(conf, sm))
  }
}


