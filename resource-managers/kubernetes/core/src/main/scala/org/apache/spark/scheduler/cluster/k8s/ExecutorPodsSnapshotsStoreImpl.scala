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

import java.util.concurrent._

import io.fabric8.kubernetes.api.model.Pod
import javax.annotation.concurrent.GuardedBy
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class ExecutorPodsSnapshotsStoreImpl(subscribersExecutor: ScheduledExecutorService)
  extends ExecutorPodsSnapshotsStore {

  private val SNAPSHOT_LOCK = new Object()

  private val subscribers = mutable.Buffer.empty[SnapshotsSubscriber]
  private val pollingTasks = mutable.Buffer.empty[Future[_]]

  @GuardedBy("SNAPSHOT_LOCK")
  private var currentSnapshot = ExecutorPodsSnapshot()

  override def addSubscriber(
      processBatchIntervalMillis: Long)
      (onNewSnapshots: Seq[ExecutorPodsSnapshot] => Unit): Unit = {
    val newSubscriber = SnapshotsSubscriber(
        new LinkedBlockingQueue[ExecutorPodsSnapshot](), onNewSnapshots)
    subscribers += newSubscriber
    pollingTasks += subscribersExecutor.scheduleWithFixedDelay(
      toRunnable(() => callSubscriber(newSubscriber)),
      0L,
      processBatchIntervalMillis,
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    pollingTasks.foreach(_.cancel(true))
    ThreadUtils.shutdown(subscribersExecutor)
  }

  override def updatePod(updatedPod: Pod): Unit = SNAPSHOT_LOCK.synchronized {
    currentSnapshot = currentSnapshot.withUpdate(updatedPod)
    addCurrentSnapshotToSubscribers()
  }

  override def replaceSnapshot(newSnapshot: Seq[Pod]): Unit = SNAPSHOT_LOCK.synchronized {
    currentSnapshot = ExecutorPodsSnapshot(newSnapshot)
    addCurrentSnapshotToSubscribers()
  }

  private def addCurrentSnapshotToSubscribers(): Unit = {
    subscribers.foreach { subscriber =>
      subscriber.snapshotsBuffer.add(currentSnapshot)
    }
  }

  private def callSubscriber(subscriber: SnapshotsSubscriber): Unit = {
    Utils.tryLogNonFatalError {
      val currentSnapshots = mutable.Buffer.empty[ExecutorPodsSnapshot].asJava
      subscriber.snapshotsBuffer.drainTo(currentSnapshots)
      subscriber.onNewSnapshots(currentSnapshots.asScala)
    }
  }

  private def toRunnable[T](runnable: () => Unit): Runnable = new Runnable {
    override def run(): Unit = runnable()
  }

  private case class SnapshotsSubscriber(
      snapshotsBuffer: BlockingQueue[ExecutorPodsSnapshot],
      onNewSnapshots: (Seq[ExecutorPodsSnapshot] => Unit))
}
