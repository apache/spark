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

import java.net.{HttpURLConnection, SocketTimeoutException, URL}
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.language.postfixOps

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import sys.process._

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage
import org.apache.spark.network.shuffle.protocol.mesos.RegisterDriver
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.ThreadUtils


/**
 * An RPC endpoint that receives registration requests from Spark drivers running on Mesos.
 * It detects driver termination and calls the cleanup callback to [[ExternalShuffleService]].
 */
private[mesos] class MesosExternalShuffleBlockHandler(
  transportConf: TransportConf, sparkMaster: String, frameworkTimeoutMs: Long)
  extends ExternalShuffleBlockHandler(transportConf, null) with Logging {

  private val cleanerThreadExecutor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("mesos-shuffle-cleaner")

  // Stores the active frameworks and when they were last seen active
  private val connectedApps = new ConcurrentHashMap[String, Long]()

  cleanerThreadExecutor.scheduleAtFixedRate(
    new MesosFrameworkCleaner(), 0, frameworkTimeoutMs / 4, TimeUnit.MILLISECONDS)

  protected override def handleMessage(
      message: BlockTransferMessage,
      client: TransportClient,
      callback: RpcResponseCallback): Unit = {
    message match {
      case RegisterDriverParam(appId) =>
        val address = client.getSocketAddress
        logDebug(s"Received registration request from app $appId (remote address $address).")
        if (connectedApps.contains(appId)) {
          logError(s"App '$appId' has re-registered.")
        }
        connectedApps.put(appId, System.nanoTime())
        callback.onSuccess(ByteBuffer.allocate(0))
      case _ => super.handleMessage(message, client, callback)
    }
  }

  /**
   * On connection termination, clean up shuffle files written by the associated application.
   */
  override def channelInactive(client: TransportClient): Unit = {
    val address = client.getSocketAddress
    logInfo(s"Socket disconnected (address was $address).")
  }

  /** An extractor object for matching [[RegisterDriver]] message. */
  private object RegisterDriverParam {
    def unapply(r: RegisterDriver): Option[String] = Some(r.getAppId)
  }

  private class MesosFrameworkCleaner extends Runnable {

    // relevant if Mesos is running in HA mode with zookeeper
    private var mesosHaMode = sparkMaster.toLowerCase().startsWith("mesos://zk://")

    // The Zookeeper URI if mesos is running in HA mode
    // (e.g. zk://zk1:port1,zk2:port2,zk3:port3/mesos)
    private var zkUri = if (!mesosHaMode) {
      None
    } else {
      Some(sparkMaster.toLowerCase().stripPrefix("mesos://"))
    }

    // The currently known mesos leader.
    private var mesosLeader: String = if (!mesosHaMode) {
      // configured as non-HA. Verify:
      val sparkMasterUri = sparkMaster.stripPrefix("mesos://")
      getMasterStateObj(sparkMasterUri) match {
        case None =>
          logError(s"Unable to retrieve mesos state on start-up from $sparkMaster (non-HA " +
            s"configuration). Verify that spark.master points to a running mesos master and " +
            s"restart the shuffle service.")
          System.exit(-1)
          sparkMasterUri
        case Some(stateObj) =>
          getZkFromStateObj(stateObj) match {
            case Some(zk) =>
              logWarning(s"Shuffle service was started with a non-HA master ($sparkMaster) but a " +
                s"HA configuration was detected. Reconfiguring shuffle service to use " +
                s"'mesos://$zk' as 'spark.master'. You might want to fix your configuration.")
              mesosHaMode = true
              zkUri = Some(zk)
              getLeaderFromZk(zkUri.get)
            case None =>
              // Started as non-HA. Detected non-HA.
              sparkMasterUri
          }
      }
    } else {
      getLeaderFromZk(zkUri.get)
    }

    lazy val objectMapper = new ObjectMapper()


    private def getLeaderFromZk(zkUri: String): String = {
      // this throws "java.lang.RuntimeException: Nonzero exit value: 255"
      // if the leader can't be determined within a timeout (5 seconds)
      val leaderFromZk = (s"mesos-resolve ${zkUri}" !!).stripLineEnd
      logTrace(s"Retrieved mesos leader $leaderFromZk from Zookeeper.")
      leaderFromZk
    }

    private def getMasterStateObj(master: String): Option[JsonNode] = {
      val stateUrl = new URL(s"http://${master}/master/state.json")
      try {
        val conn = stateUrl.openConnection().asInstanceOf[HttpURLConnection]
        conn.setRequestMethod("GET")
        conn.setConnectTimeout(5000) // 5 secs
        if (200 == conn.getResponseCode) {
          Some(objectMapper.readTree(conn.getInputStream))
        } else {
          None
        }
      } catch {
        case _: SocketTimeoutException =>
          logError(s"Connection to mesos leader at $stateUrl timed out.")
          None
      }
    }

    private def getLeaderFromStateObj(stateObj: JsonNode): Option[String] = {
      if (stateObj.has("leader")) {
        Some(stateObj.get("leader").asText().stripPrefix("master@"))
      } else {
        None
      }
    }

    private def getRunningFrameworks(stateObj: JsonNode): Set[String] = {
      stateObj.get("frameworks").elements().asScala
        .map(_.get("id").asText()).toSet
    }

    private def getZkFromStateObj(stateObj: JsonNode): Option[String] = {
      val flags = stateObj.get("flags")
      if (flags.has("zk")) {
        Some(flags.get("zk").asText())
      } else {
        None
      }
    }

    override def run(): Unit = {
      getMasterStateObj(mesosLeader) match {
        case None =>
          if (mesosHaMode) {
            mesosLeader = getLeaderFromZk(zkUri.get)
            logInfo(s"Failed to retrieve mesos state, but found a new leader: $mesosLeader. " +
              s"Will retry.")
          } else {
            logError("Failed to retrieve mesos (non-HA) state.")
          }
        case Some(state) =>
          getLeaderFromStateObj(state) match {
            case None => logError("Failed to determine mesos leader from state.json")
            case Some(leader) =>
              if (leader != mesosLeader) {
                logInfo(s"Got a new leader ($leader) from state.json. Will retry with the new " +
                  s"leader.")
                mesosLeader = leader
              } else {
                // definitely got the state from the leader
                val runningFrameworks = getRunningFrameworks(state)
                val now = System.nanoTime()
                runningFrameworks.foreach { id =>
                  if (connectedApps.containsKey(id)) {
                    connectedApps.replace(id, now)
                  }
                }
                connectedApps.asScala.foreach { case (appId, lastSeen) =>
                  if (now - lastSeen > frameworkTimeoutMs * 1000 * 1000) {
                    logInfo(s"Application $appId has timed out. Removing shuffle files.")
                    applicationRemoved(appId, true)
                    connectedApps.remove(appId)
                  }
                }
              }
          }
      }
    }
  }
}

/**
 * A wrapper of [[ExternalShuffleService]] that provides an additional endpoint for drivers
 * to register with. This allows the shuffle service to detect when a mesos framework is no longer
 * running and can clean up the associated shuffle files after a timeout.
 */
private[mesos] class MesosExternalShuffleService(conf: SparkConf, securityManager: SecurityManager)
  extends ExternalShuffleService(conf, securityManager) {

  protected override def newShuffleBlockHandler(
      conf: TransportConf): ExternalShuffleBlockHandler = {
    new MesosExternalShuffleBlockHandler(
      conf,
      this.conf.get("spark.master"),
      this.conf.getTimeAsMs("spark.storage.blockManagerSlaveTimeoutMs", "120s"))
  }
}

private[spark] object MesosExternalShuffleService extends Logging {

  def main(args: Array[String]): Unit = {
    ExternalShuffleService.main(args,
      (conf: SparkConf, sm: SecurityManager) => new MesosExternalShuffleService(conf, sm))
  }
}


