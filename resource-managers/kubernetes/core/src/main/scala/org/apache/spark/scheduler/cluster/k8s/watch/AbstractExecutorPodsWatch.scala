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

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.deploy.k8s.{Constants, KubernetesStepUtil}
import org.apache.spark.scheduler.cluster.k8s.{ExecutorPodsSnapshotsStore, ExecutorPodsWatchSnapshotSource}

private[k8s] trait AbstractExecutorPodsWatch {
  def start(applicationId: String): Unit
  def stop(): Unit
}

private[k8s] object AbstractExecutorPodsWatch {
  def create(watchType: String,
             snapshotsStore: ExecutorPodsSnapshotsStore,
             kubernetesClient: KubernetesClient,
             sc: SparkContext,
             maybeWatcherHostname: Option[String]): AbstractExecutorPodsWatch = watchType match {
    case Constants.WATCH_TYPE_INTERNAL =>
      new ExecutorPodsWatchSnapshotSource(snapshotsStore, kubernetesClient, sc.conf)
    case Constants.WATCH_TYPE_EXTERNAL =>
      val watchPort = KubernetesStepUtil.watchPort(sc.conf)
      val watchHost = maybeWatcherHostname.getOrElse(throw new SparkException(
        "You cannot start an external watcher if you do not have a hostname."
      ))
      ExecutorPodsWatchRpc.createClient(sc.env.rpcEnv, watchHost, watchPort)
    case _ => throw new IllegalArgumentException(f"The only types of pods watcher available " +
      f"are {${Constants.WATCH_TYPE_INTERNAL}, ${Constants.WATCH_TYPE_EXTERNAL}} but " +
      f"the following was given: $watchType")
  }
}
