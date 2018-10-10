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
package org.apache.spark.shuffle.k8s

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watch, Watcher}
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.k8s._
import org.apache.spark.shuffle.ShuffleServiceAddressProvider
import org.apache.spark.util.Utils

class KubernetesShuffleServiceAddressProvider(
    kubernetesClient: KubernetesClient,
    pollForPodsExecutor: ScheduledExecutorService,
    podLabels: Map[String, String],
    namespace: String,
    portNumber: Int)
  extends ShuffleServiceAddressProvider with Logging {

  // General implementation remark: this bears a strong resemblance to ExecutorPodsSnapshotsStore,
  // but we don't need all "in-between" lists of all executor pods, just the latest known list
  // when we query in getShuffleServiceAddresses.

  private val podsUpdateLock = new ReentrantReadWriteLock()

  private val shuffleServicePods = mutable.HashMap.empty[String, Pod]

  private var shuffleServicePodsWatch: Watch = _
  private var pollForPodsTask: ScheduledFuture[_] = _

  override def start(): Unit = {
    pollForPods()
    pollForPodsTask = pollForPodsExecutor.scheduleWithFixedDelay(
      () => pollForPods(), 0, 10, TimeUnit.SECONDS)
    shuffleServicePodsWatch = kubernetesClient
      .pods()
      .inNamespace(namespace)
      .withLabels(podLabels.asJava).watch(new PutPodsInCacheWatcher())
  }

  override def stop(): Unit = {
    Utils.tryLogNonFatalError {
      if (pollForPodsTask != null) {
        pollForPodsTask.cancel(false)
      }
    }

    Utils.tryLogNonFatalError {
      if (shuffleServicePodsWatch != null) {
        shuffleServicePodsWatch.close()
      }
    }

    Utils.tryLogNonFatalError {
      kubernetesClient.close()
    }
  }

  override def getShuffleServiceAddresses(): List[(String, Int)] = {
    val readLock = podsUpdateLock.readLock()
    readLock.lock()
    try {
      val addresses = shuffleServicePods.values.map(pod => {
        (pod.getStatus.getPodIP, portNumber)
      }).toList
      logInfo(s"Found backup shuffle service addresses at $addresses.")
      addresses
    } finally {
      readLock.unlock()
    }
  }

  private def pollForPods(): Unit = {
    val writeLock = podsUpdateLock.writeLock()
    writeLock.lock()
    try {
      val allPods = kubernetesClient
        .pods()
        .inNamespace(namespace)
        .withLabels(podLabels.asJava)
        .list()
      shuffleServicePods.clear()
      allPods.getItems.asScala.foreach(updatePod)
    } finally {
      writeLock.unlock()
    }
  }

  private def updatePod(pod: Pod): Unit = {
    require(podsUpdateLock.isWriteLockedByCurrentThread, "Should only update pods under lock.")
    val state = SparkPodState.toState(pod)
    state match {
      case PodPending(_) | PodFailed(_) | PodSucceeded(_) | PodDeleted(_) =>
        shuffleServicePods.remove(pod.getMetadata.getName)
      case PodRunning(_) =>
        shuffleServicePods.put(pod.getMetadata.getName, pod)
      case _ =>
        logWarning(s"Unknown state $state for pod named ${pod.getMetadata.getName}")
    }
  }

  private def deletePod(pod: Pod): Unit = {
    require(podsUpdateLock.isWriteLockedByCurrentThread, "Should only delete under lock.")
    shuffleServicePods.remove(pod.getMetadata.getName)
  }

  private class PutPodsInCacheWatcher extends Watcher[Pod] {
    override def eventReceived(action: Watcher.Action, pod: Pod): Unit = {
      val writeLock = podsUpdateLock.writeLock()
      writeLock.lock()
      try {
        updatePod(pod)
      } finally {
        writeLock.unlock()
      }
    }

    override def onClose(e: KubernetesClientException): Unit = {}
  }

  private implicit def toRunnable(func: () => Unit): Runnable = {
    new Runnable {
      override def run(): Unit = func()
    }
  }
}
