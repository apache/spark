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
package org.apache.spark.scheduler.cluster.k8s.watch

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.SparkContext
import org.apache.spark.deploy.k8s.Constants
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}

private[k8s] sealed trait PodsWatchMessage extends Serializable

private[k8s] case class StartMessage(applicationID: String) extends PodsWatchMessage
private[k8s] case object StopMessage extends PodsWatchMessage

/**
 * This endpoint can be used to start and stop the remote ExecutorPodsWatch application. This
 * is used so the cluster manager node can tell the executor pods watch node when to start
 * and stop the watching of nodes.
 */
private[k8s] class RemoteExecutorPodsWatchEndpoint(override val rpcEnv: RpcEnv,
                           localExecutorWatch: AbstractExecutorPodsWatch)
  extends ThreadSafeRpcEndpoint {

  override def receive: PartialFunction[Any, Unit] = {
    case StartMessage(applicationID) =>
      localExecutorWatch.start(applicationID)
    case StopMessage =>
      localExecutorWatch.stop()
  }
}

/**
 * This client can be used to tell the remote ExecutorPodsWatch application to start and
 * stop watching the nodes.
 */
private[k8s] class RemoteExecutorPodsWatchClient(rpcEnv: RpcEnv, podsWatchEndpoint: RpcEndpointRef)
  extends AbstractExecutorPodsWatch {
  override def start(applicationId: String): Unit = {
    podsWatchEndpoint.askSync(StartMessage(applicationId))
  }

  override def stop(): Unit = {
    podsWatchEndpoint.askSync(StopMessage)
    rpcEnv.shutdown()
  }
}

private[k8s] object ExecutorPodsWatchRpc {
  /**
   * Create the ExecutorPodsWatch backend endpoint. This lives on the external watcher and is
   * used to watch the executors from afar.
   */
  def createBackendEndpoint(rpcEnv: RpcEnv,
                            sc: SparkContext,
                            kubernetesClient: KubernetesClient): RemoteExecutorPodsWatchEndpoint = {
    // We need to access the snapshot client from the driver
    val snapshotClient = SnapshotStoreRpc.createClient(rpcEnv, sc.conf)
    // So it can be used by the local executor pods watcher to update the snapshot source on the
    // cluster manager
    val localExecutorPodsWatcher = AbstractExecutorPodsWatch.create(
      watchType = Constants.WATCH_TYPE_INTERNAL,
      snapshotsStore = snapshotClient,
      kubernetesClient = kubernetesClient,
      sc = sc,
      maybeWatcherHostname = None)

    new RemoteExecutorPodsWatchEndpoint(rpcEnv, localExecutorPodsWatcher)
  }

  /**
   * Create the client to the remote executor pods watch. This will live in the cluster manager
   * and is used to start and stop the remote executor pods watch from afar.
   */
  def createClient(rpcEnv: RpcEnv,
                   watchHost: String,
                   watchPort: Int): RemoteExecutorPodsWatchClient = {
    var podsWatchEndpoint: RpcEndpointRef = null

    val nTries = sys.env.getOrElse("EXECUTOR_DRIVER_PROPS_FETCHER_MAX_ATTEMPTS", "3").toInt
    for (i <- 0 until nTries if podsWatchEndpoint == null) {
      try {
        podsWatchEndpoint = rpcEnv.setupEndpointRef(RpcAddress(watchHost, watchPort),
          Constants.WATCH_ENDPOINT_NAME)
      } catch {
        case e: Throwable => if (i == nTries - 1) {
          throw e
        }
      }
    }

    new RemoteExecutorPodsWatchClient(rpcEnv, podsWatchEndpoint)
  }
}
