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
package org.apache.spark.scheduler.cluster.k8s

import scala.collection.mutable

import io.fabric8.kubernetes.api.model.Pod

import org.apache.spark.deploy.k8s.Constants.DEFAULT_EXECUTOR_CONTAINER_NAME
import org.apache.spark.util.ManualClock

class DeterministicExecutorPodsSnapshotsStore extends ExecutorPodsSnapshotsStore {

  ExecutorPodsSnapshot.setShouldCheckAllContainers(false)
  ExecutorPodsSnapshot.setSparkContainerName(DEFAULT_EXECUTOR_CONTAINER_NAME)

  val clock = new ManualClock()

  private val snapshotsBuffer = mutable.Buffer.empty[ExecutorPodsSnapshot]
  private val subscribers = mutable.Buffer.empty[Seq[ExecutorPodsSnapshot] => Unit]

  private var currentSnapshot = ExecutorPodsSnapshot()

  override def addSubscriber
      (processBatchIntervalMillis: Long)
      (onNewSnapshots: Seq[ExecutorPodsSnapshot] => Unit): Unit = {
    subscribers += onNewSnapshots
  }

  override def stop(): Unit = {}

  override def notifySubscribers(): Unit = {
    subscribers.foreach(_(snapshotsBuffer.toSeq))
    snapshotsBuffer.clear()
  }

  override def updatePod(updatedPod: Pod): Unit = {
    currentSnapshot = currentSnapshot.withUpdate(updatedPod)
    snapshotsBuffer += currentSnapshot
  }

  override def replaceSnapshot(newSnapshot: Seq[Pod]): Unit = {
    currentSnapshot = ExecutorPodsSnapshot(newSnapshot, clock.getTimeMillis())
    snapshotsBuffer += currentSnapshot
  }

  def removeDeletedExecutors(): Unit = {
    val nonDeleted = currentSnapshot.executorPods.filter {
      case (_, PodDeleted(_)) => false
      case _ => true
    }
    currentSnapshot = ExecutorPodsSnapshot(nonDeleted, clock.getTimeMillis())
    snapshotsBuffer += currentSnapshot
  }
}
