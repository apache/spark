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

import io.fabric8.kubernetes.api.model.Pod

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Constants
import org.apache.spark.deploy.k8s.Constants.DEFAULT_DRIVER_PORT
import org.apache.spark.internal.config.{DRIVER_HOST_ADDRESS, DRIVER_PORT}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.cluster.k8s.{ExecutorPodsSnapshot, ExecutorPodsSnapshotsStore}

private[k8s] case class UpdatePodMessage(updatedPod: Pod)

/**
 * This endpoint can be used to update the local ExecutorPodsSnapshotsStore from external
 * nodes. This is currently required so the remote ExecutorPodsWatch can update the local store
 * on the cluster manager node.
 */
private[k8s] class RemoteSnapshotStoreEndpoint(override val rpcEnv: RpcEnv,
                                               localStore: ExecutorPodsSnapshotsStore)
  extends ThreadSafeRpcEndpoint {

  override def receive: PartialFunction[Any, Unit] = {
    case UpdatePodMessage(updatedPod) =>
      localStore.updatePod(updatedPod)
  }
}

/**
 * This client can be used to update pod information on the cluster manager node. This
 * is currently required so the remote ExecutorPodsWatch can send information to the local
 * store on the cluster manager node.
 */
private[k8s] class RemoteSnapshotStoreClient(rpcEnv: RpcEnv, snapshotStoreEndpoint: RpcEndpointRef)
  extends ExecutorPodsSnapshotsStore {
  override def addSubscriber(processBatchIntervalMillis: Long)
                            (onNewSnapshots: Seq[ExecutorPodsSnapshot] => Unit): Unit = {
    throw new SparkException(s"RemoteSnapshotStoreClient does not implement 'addSubscriber'")
  }

  override def stop(): Unit = {
    rpcEnv.shutdown()
  }

  override def notifySubscribers(): Unit = {
    throw new SparkException(s"RemoteSnapshotStoreClient does not implement 'notifySubscribers'")
  }

  override def updatePod(updatedPod: Pod): Unit = {
    snapshotStoreEndpoint.askSync(UpdatePodMessage(updatedPod))
  }

  override def replaceSnapshot(newSnapshot: Seq[Pod]): Unit = {
    throw new SparkException(s"RemoteSnapshotStoreClient does not implement 'replaceSnapshot'")
  }
}

private[k8s] object SnapshotStoreRpc {
  /**
   * Create the snapshot source backend. This should live in the cluster manager and should
   * update the cluster manager's local snapshot store. It allows external containers to
   * update the store remotely.
   */
  def createBackendEndpoint(rpcEnv: RpcEnv, localStore: ExecutorPodsSnapshotsStore
                    ): RemoteSnapshotStoreEndpoint = {
    new RemoteSnapshotStoreEndpoint(rpcEnv, localStore)
  }

  /**
   * Create the client to the snapshot source backend. This will live on the external watcher
   * and can be used to update the cluster manager's snapshot store remotely.
   */
  def createClient(rpcEnv: RpcEnv, conf: SparkConf): RemoteSnapshotStoreClient = {
    var storeEndpoint: RpcEndpointRef = null

    val nTries = sys.env.getOrElse("EXECUTOR_DRIVER_PROPS_FETCHER_MAX_ATTEMPTS", "3").toInt
    for (i <- 0 until nTries if storeEndpoint == null) {
      try {
        storeEndpoint = rpcEnv.setupEndpointRef(
          address = RpcAddress(
            host = conf.get(DRIVER_HOST_ADDRESS),
            port = conf.getInt(DRIVER_PORT.key, DEFAULT_DRIVER_PORT)),
          endpointName = Constants.STORE_ENDPOINT_NAME)
      } catch {
        case e: Throwable => if (i == nTries - 1) {
          throw e
        }
      }
    }

    new RemoteSnapshotStoreClient(rpcEnv, storeEndpoint)
  }
}
