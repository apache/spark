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

import java.util.ArrayList
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model.Pod

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_EXECUTOR_SNAPSHOTS_SUBSCRIBERS_GRACE_PERIOD
import org.apache.spark.internal.Logging
import org.apache.spark.util.Clock
import org.apache.spark.util.SystemClock
import org.apache.spark.util.ThreadUtils

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
 * <br>
 * The subscriber notification callback is guaranteed to be called from a single thread at a time.
 */
private[spark] class ExecutorPodsSnapshotsStoreImpl(
    subscribersExecutor: ScheduledExecutorService,
    clock: Clock = new SystemClock,
    conf: SparkConf = SparkContext.getActive.get.conf)
  extends ExecutorPodsSnapshotsStore with Logging {

  private val SNAPSHOT_LOCK = new Object()

  private val subscribers = new CopyOnWriteArrayList[SnapshotsSubscriber]()
  private val pollingTasks = new CopyOnWriteArrayList[Future[_]]

  @GuardedBy("SNAPSHOT_LOCK")
  private var currentSnapshot = ExecutorPodsSnapshot()

  override def addSubscriber(
      processBatchIntervalMillis: Long)
      (onNewSnapshots: Seq[ExecutorPodsSnapshot] => Unit): Unit = {
    val newSubscriber = new SnapshotsSubscriber(onNewSnapshots)
    SNAPSHOT_LOCK.synchronized {
      newSubscriber.addCurrentSnapshot()
    }
    subscribers.add(newSubscriber)
    pollingTasks.add(subscribersExecutor.scheduleWithFixedDelay(
      () => newSubscriber.processSnapshots(),
      0L,
      processBatchIntervalMillis,
      TimeUnit.MILLISECONDS))
  }

  override def notifySubscribers(): Unit = SNAPSHOT_LOCK.synchronized {
    subscribers.asScala.foreach { s =>
      subscribersExecutor.submit(new Runnable() {
        override def run(): Unit = s.processSnapshots()
      })
    }
  }

  override def stop(): Unit = {
    pollingTasks.asScala.foreach(_.cancel(false))
    val awaitSeconds = conf.get(KUBERNETES_EXECUTOR_SNAPSHOTS_SUBSCRIBERS_GRACE_PERIOD)
    ThreadUtils.shutdown(subscribersExecutor, FiniteDuration(awaitSeconds, TimeUnit.SECONDS))
  }

  override def updatePod(updatedPod: Pod): Unit = SNAPSHOT_LOCK.synchronized {
    currentSnapshot = currentSnapshot.withUpdate(updatedPod)
    addCurrentSnapshotToSubscribers()
  }

  override def replaceSnapshot(newSnapshot: Seq[Pod]): Unit = SNAPSHOT_LOCK.synchronized {
    currentSnapshot = ExecutorPodsSnapshot(newSnapshot, clock.getTimeMillis())
    addCurrentSnapshotToSubscribers()
  }

  private def addCurrentSnapshotToSubscribers(): Unit = {
    subscribers.asScala.foreach(_.addCurrentSnapshot())
  }

  private class SnapshotsSubscriber(onNewSnapshots: Seq[ExecutorPodsSnapshot] => Unit) {

    private val snapshotsBuffer = new LinkedBlockingQueue[ExecutorPodsSnapshot]()
    private val lock = new ReentrantLock()
    private val notificationCount = new AtomicInteger()

    def addCurrentSnapshot(): Unit = {
      snapshotsBuffer.add(currentSnapshot)
    }

    def processSnapshots(): Unit = {
      notificationCount.incrementAndGet()
      processSnapshotsInternal()
    }

    private def processSnapshotsInternal(): Unit = {
      if (lock.tryLock()) {
        // Check whether there are pending notifications before calling the subscriber. This
        // is needed to avoid calling the subscriber spuriously when the race described in the
        // comment below happens.
        if (notificationCount.get() > 0) {
          try {
            val snapshots = new ArrayList[ExecutorPodsSnapshot]()
            snapshotsBuffer.drainTo(snapshots)
            onNewSnapshots(snapshots.asScala.toSeq)
          } catch {
            case e: IllegalArgumentException =>
              logError("Going to stop due to IllegalArgumentException", e)
              System.exit(1)
            case NonFatal(e) => logWarning("Exception when notifying snapshot subscriber.", e)
          } finally {
            lock.unlock()
          }

          if (notificationCount.decrementAndGet() > 0) {
            // There was another concurrent request for this subscriber. Schedule a task to
            // immediately process snapshots again, so that the subscriber can pick up any
            // changes that may have happened between the time it started looking at snapshots
            // above, and the time the concurrent request arrived.
            //
            // This has to be done outside of the lock, otherwise we might miss a notification
            // arriving after the above check, but before we've released the lock. Flip side is
            // that we may schedule a useless task that will just fail to grab the lock.
            subscribersExecutor.submit(new Runnable() {
              override def run(): Unit = processSnapshotsInternal()
            })
          }
        } else {
          lock.unlock()
        }
      }
    }
  }
}
