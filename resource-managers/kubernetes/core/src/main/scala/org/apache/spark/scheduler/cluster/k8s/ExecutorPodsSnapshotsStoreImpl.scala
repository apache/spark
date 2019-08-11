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

/**
 * Controls the propagation of the Spark application's executor pods state to subscribers that
 * react to that state.
 * <br>
 * Roughly follows a producer-consumer model. Producers report states of executor pods, and these
 * states are then published to consumers that can perform any actions in response to these states.
 * <br>
 * Producers push updates in one of two ways. An incremental update sent by updatePod() represents
 * a known new state of a single executor pod. A full sync sent by replaceSnapshot() indicates that
 * the passed pods are all of the most up to date states of all executor pods for the application.
 * The combination of the states of all executor pods for the application is collectively known as
 * a snapshot. The store keeps track of the most up to date snapshot, and applies updates to that
 * most recent snapshot - either by incrementally updating the snapshot with a single new pod state,
 * or by replacing the snapshot entirely on a full sync.
 * <br>
 * Consumers, or subscribers, register that they want to be informed about all snapshots of the
 * executor pods. Every time the store replaces its most up to date snapshot from either an
 * incremental update or a full sync, the most recent snapshot after the update is posted to the
 * subscriber's buffer. Subscribers receive blocks of snapshots produced by the producers in
 * time-windowed chunks. Each subscriber can choose to receive their snapshot chunks at different
 * time intervals.
 */
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
    SNAPSHOT_LOCK.synchronized {
      newSubscriber.snapshotsBuffer.add(currentSnapshot)
    }
    subscribers += newSubscriber
    pollingTasks += subscribersExecutor.scheduleWithFixedDelay(
      () => callSubscriber(newSubscriber),
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

  private case class SnapshotsSubscriber(
      snapshotsBuffer: BlockingQueue[ExecutorPodsSnapshot],
      onNewSnapshots: Seq[ExecutorPodsSnapshot] => Unit)
}
