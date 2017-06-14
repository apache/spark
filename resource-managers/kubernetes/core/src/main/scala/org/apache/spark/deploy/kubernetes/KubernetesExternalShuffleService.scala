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

package org.apache.spark.deploy.kubernetes

import java.io.File
import java.nio.ByteBuffer

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{Config, KubernetesClient, KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.apache.commons.io.IOUtils
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, RegisterDriver}
import org.apache.spark.network.util.TransportConf

/**
 * An RPC endpoint that receives registration requests from Spark drivers running on Kubernetes.
 * It detects driver termination and calls the cleanup callback to [[ExternalShuffleService]].
 */
private[spark] class KubernetesShuffleBlockHandler (
    transportConf: TransportConf,
    kubernetesClient: KubernetesClient)
  extends ExternalShuffleBlockHandler(transportConf, null) with Logging {

  private val INIT_AND_STOP_LOCK = new Object
  private val CONNECTED_APPS_LOCK = new Object
  private val connectedApps = mutable.Set.empty[String]
  private var shuffleWatch: Option[Watch] = None

  def start(): Unit = INIT_AND_STOP_LOCK.synchronized {
    shuffleWatch = startShuffleWatcher()
  }

  override def close(): Unit = {
    try {
      super.close()
    } finally {
      INIT_AND_STOP_LOCK.synchronized {
        shuffleWatch.foreach(IOUtils.closeQuietly)
        shuffleWatch = None
        IOUtils.closeQuietly(kubernetesClient)
      }
    }
  }

  protected override def handleMessage(
    message: BlockTransferMessage,
    client: TransportClient,
    callback: RpcResponseCallback): Unit = {
      message match {
        case RegisterDriverParam(appId) =>
          val address = client.getSocketAddress
          logInfo(s"Received registration request from app $appId (remote address $address).")
          CONNECTED_APPS_LOCK.synchronized {
            if (connectedApps.contains(appId)) {
              logWarning(s"Received a registration request from app $appId, but it was already " +
                s"registered")
            }
            connectedApps += appId
          }
          callback.onSuccess(ByteBuffer.allocate(0))
        case _ => super.handleMessage(message, client, callback)
      }
  }

  private def startShuffleWatcher(): Option[Watch] = {
    try {
      Some(kubernetesClient
        .pods()
        .withLabels(Map(SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE).asJava)
        .watch(new Watcher[Pod] {
          override def eventReceived(action: Watcher.Action, p: Pod): Unit = {
            action match {
              case Action.DELETED | Action.ERROR =>
                val labels = p.getMetadata.getLabels
                if (labels.containsKey(SPARK_APP_ID_LABEL)) {
                  val appId = labels.get(SPARK_APP_ID_LABEL)
                  CONNECTED_APPS_LOCK.synchronized {
                    if (connectedApps.contains(appId)) {
                      connectedApps -= appId
                      applicationRemoved(appId, true)
                    }
                  }
                }
              case Action.ADDED | Action.MODIFIED =>
            }
          }

          override def onClose(e: KubernetesClientException): Unit = {}
        }))
    } catch {
      case throwable: Throwable =>
        logWarning(s"Shuffle service cannot access Kubernetes. " +
          s"Orphaned file cleanup is disabled.", throwable)
        None
    }
  }

  /** An extractor object for matching [[RegisterDriver]] message. */
  private object RegisterDriverParam {
    def unapply(r: RegisterDriver): Option[(String)] =
      Some(r.getAppId)
  }
}

/**
 * A wrapper of [[ExternalShuffleService]] that provides an additional endpoint for drivers
 * to associate with. This allows the shuffle service to detect when a driver is terminated
 * and can clean up the associated shuffle files.
 */
private[spark] class KubernetesExternalShuffleService(
    conf: SparkConf,
    securityManager: SecurityManager)
  extends ExternalShuffleService(conf, securityManager) {

  private var shuffleBlockHandlers: mutable.Buffer[KubernetesShuffleBlockHandler] = _
  protected override def newShuffleBlockHandler(
      tConf: TransportConf): ExternalShuffleBlockHandler = {
    val kubernetesClient = SparkKubernetesClientFactory.createKubernetesClient(
        conf.get(KUBERNETES_SHUFFLE_APISERVER_URI),
        None,
        APISERVER_AUTH_SHUFFLE_SERVICE_CONF_PREFIX,
        conf,
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH))
            .filter( _ => conf.get(KUBERNETES_SHUFFLE_USE_SERVICE_ACCOUNT_CREDENTIALS)),
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH))
            .filter( _ => conf.get(KUBERNETES_SHUFFLE_USE_SERVICE_ACCOUNT_CREDENTIALS)))
    val newBlockHandler = new KubernetesShuffleBlockHandler(tConf, kubernetesClient)
    try {
      newBlockHandler.start()
      // TODO: figure out a better way of doing this.
      // This is necessary because the constructor is not called
      // when this class is initialized through ExternalShuffleService.
      if (shuffleBlockHandlers == null) {
        shuffleBlockHandlers = mutable.Buffer.empty[KubernetesShuffleBlockHandler]
      }
      shuffleBlockHandlers += newBlockHandler
      newBlockHandler
    } catch {
      case e: Throwable =>
        logError("Failed to create Kubernetes shuffle block handler.", e)
        newBlockHandler.close()
        throw e
    }
  }

  override def stop(): Unit = {
    try {
      super.stop()
    } finally {
      if (shuffleBlockHandlers != null) {
        shuffleBlockHandlers.foreach(_.close())
      }
    }
  }
}

private[spark] object KubernetesExternalShuffleService extends Logging {
  def main(args: Array[String]): Unit = {
    ExternalShuffleService.main(args,
      (conf: SparkConf, sm: SecurityManager) => new KubernetesExternalShuffleService(conf, sm))
  }
}


