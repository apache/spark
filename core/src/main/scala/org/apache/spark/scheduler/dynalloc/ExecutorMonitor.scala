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

package org.apache.spark.scheduler.dynalloc

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.scheduler._
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.Clock

/**
 * A monitor for executor activity, used by ExecutorAllocationManager to detect idle executors.
 */
private[spark] class ExecutorMonitor(
    conf: SparkConf,
    client: ExecutorAllocationClient,
    clock: Clock) extends SparkListener with Logging {

  private val idleTimeoutMs = TimeUnit.SECONDS.toMillis(
    conf.get(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT))
  private val storageTimeoutMs = TimeUnit.SECONDS.toMillis(
    conf.get(DYN_ALLOCATION_CACHED_EXECUTOR_IDLE_TIMEOUT))
  private val fetchFromShuffleSvcEnabled = conf.get(SHUFFLE_SERVICE_ENABLED) &&
    conf.get(SHUFFLE_SERVICE_FETCH_RDD_ENABLED)

  private val executors = new ConcurrentHashMap[String, Tracker]()

  // The following fields are an optimization to avoid having to scan all executors on every EAM
  // schedule interval to find out which ones are timed out. They keep track of when the next
  // executor timeout is expected to happen, and the current list of timed out executors. There's
  // also a flag that forces the EAM task to recompute the timed out executors, in case some event
  // arrives on the listener bus that may cause the current list of timed out executors to change.
  //
  // There's also per-executor state used for this purpose, so that recomputations can be triggered
  // only when really necessary.
  //
  // Note that this isn't meant to, and cannot, always make the right decision about which executors
  // are indeed timed out. For example, the EAM thread may detect a timed out executor while a new
  // "task start" event has just been posted to the listener bus and hasn't yet been delivered to
  // this listener. There are safeguards in other parts of the code that would prevent that executor
  // from being removed.
  private val nextTimeout = new AtomicLong(Long.MaxValue)
  private var timedOutExecs = Seq.empty[String]

  def reset(): Unit = {
    executors.clear()
    nextTimeout.set(Long.MaxValue)
    timedOutExecs = Nil
  }

  /**
   * Returns the list of executors that are currently considered to be timed out.
   * Should only be called from the EAM thread.
   */
  def timedOutExecutors(): Seq[String] = {
    val now = clock.getTimeMillis()
    if (now >= nextTimeout.get()) {
      // Temporarily set the next timeout at Long.MaxValue. This ensures that after
      // scanning all executors below, we know when the next timeout for non-timed out
      // executors is (whether that update came from the scan, or from a new event
      // arriving in a different thread).
      nextTimeout.set(Long.MaxValue)

      var newNextTimeout = Long.MaxValue
      timedOutExecs = executors.asScala
        .filter { case (_, exec) => !exec.pendingRemoval }
        .filter { case (_, exec) =>
          val deadline = exec.timeoutAt
          if (deadline > now) {
            newNextTimeout = math.min(newNextTimeout, deadline)
            exec.timedOut = false
            false
          } else {
            exec.timedOut = true
            true
          }
        }
        .keys
        .toSeq
      updateNextTimeout(newNextTimeout)
    }
    timedOutExecs
  }

  /**
   * Mark the given executors as pending to be removed. Should only be called in the EAM thread.
   */
  def executorsKilled(ids: Seq[String]): Unit = {
    ids.foreach { id =>
      val tracker = executors.get(id)
      if (tracker != null) {
        tracker.pendingRemoval = true
      }
    }

    // Recompute timed out executors in the next EAM callback, since this call invalidates
    // the current list.
    nextTimeout.set(Long.MinValue)
  }

  def executorCount: Int = executors.size()

  def pendingRemovalCount: Int = executors.asScala.count { case (_, exec) => exec.pendingRemoval }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    val executorId = event.taskInfo.executorId
    // Guard against a late arriving task start event (SPARK-26927).
    if (client.isExecutorActive(executorId)) {
      val exec = ensureExecutorIsTracked(executorId)
      exec.updateRunningTasks(1)
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val executorId = event.taskInfo.executorId
    val exec = executors.get(executorId)
    if (exec != null) {
      exec.updateRunningTasks(-1)
    }
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    val exec = ensureExecutorIsTracked(event.executorId)
    exec.updateRunningTasks(0)
    logInfo(s"New executor ${event.executorId} has registered (new total is ${executors.size()})")
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    val removed = executors.remove(event.executorId)
    if (removed != null) {
      logInfo(s"Executor ${event.executorId} removed (new total is ${executors.size()})")
      if (!removed.pendingRemoval) {
        nextTimeout.set(Long.MinValue)
      }
    }
  }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    if (!event.blockUpdatedInfo.blockId.isInstanceOf[RDDBlockId]) {
      return
    }

    val exec = ensureExecutorIsTracked(event.blockUpdatedInfo.blockManagerId.executorId)
    val storageLevel = event.blockUpdatedInfo.storageLevel
    val blockId = event.blockUpdatedInfo.blockId.asInstanceOf[RDDBlockId]

    // SPARK-27677. When a block can be fetched from the external shuffle service, the executor can
    // be removed without hurting the application too much, since the cached data is still
    // available. So don't count blocks that can be served by the external service.
    if (storageLevel.isValid && (!fetchFromShuffleSvcEnabled || !storageLevel.useDisk)) {
      val hadCachedBlocks = exec.cachedBlocks.nonEmpty

      val blocks = exec.cachedBlocks.getOrElseUpdate(blockId.rddId,
        new mutable.BitSet(blockId.splitIndex))
      blocks += blockId.splitIndex

      if (!hadCachedBlocks) {
        exec.updateTimeout()
      }
    } else {
      exec.cachedBlocks.get(blockId.rddId).foreach { blocks =>
        blocks -= blockId.splitIndex
        if (blocks.isEmpty) {
          exec.cachedBlocks -= blockId.rddId
          if (exec.cachedBlocks.isEmpty) {
            exec.updateTimeout()
          }
        }
      }
    }
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    executors.values().asScala.foreach { exec =>
      exec.cachedBlocks -= event.rddId
      if (exec.cachedBlocks.isEmpty) {
        exec.updateTimeout()
      }
    }
  }

  // Visible for testing.
  private[dynalloc] def isExecutorIdle(id: String): Boolean = {
    Option(executors.get(id)).map(_.isIdle).getOrElse(throw new NoSuchElementException(id))
  }

  // Visible for testing
  private[dynalloc] def timedOutExecutors(when: Long): Seq[String] = {
    executors.asScala.flatMap { case (id, tracker) =>
      if (tracker.timeoutAt <= when) Some(id) else None
    }.toSeq
  }

  // Visible for testing
  def executorsPendingToRemove(): Set[String] = {
    executors.asScala.filter { case (_, exec) => exec.pendingRemoval }.keys.toSet
  }

  /**
   * This method should be used when updating executor state. It guards against a race condition in
   * which the `SparkListenerTaskStart` event is posted before the `SparkListenerBlockManagerAdded`
   * event, which is possible because these events are posted in different threads. (see SPARK-4951)
   */
  private def ensureExecutorIsTracked(id: String): Tracker = {
    executors.computeIfAbsent(id, _ => new Tracker())
  }

  private def updateNextTimeout(newValue: Long): Unit = {
    while (true) {
      val current = nextTimeout.get()
      if (newValue >= current || nextTimeout.compareAndSet(current, newValue)) {
        return
      }
    }
  }

  private class Tracker {
    @volatile var timeoutAt: Long = Long.MaxValue

    // Tracks whether this executor is thought to be timed out. It's used to detect when the list
    // of timed out executors needs to be updated due to the executor's state changing.
    @volatile var timedOut: Boolean = false

    var pendingRemoval: Boolean = false

    private var idleStart: Long = -1
    private var runningTasks: Int = 0

    // Maps RDD IDs to the partition IDs stored in the executor.
    // This should only be used in the event thread.
    val cachedBlocks = new mutable.HashMap[Int, mutable.BitSet]()

    // For testing.
    def isIdle: Boolean = idleStart >= 0

    def updateRunningTasks(delta: Int): Unit = {
      runningTasks = math.max(0, runningTasks + delta)
      idleStart = if (runningTasks == 0) clock.getTimeMillis() else -1L
      updateTimeout()
    }

    def updateTimeout(): Unit = {
      val oldDeadline = timeoutAt
      val newDeadline = if (idleStart >= 0) {
        idleStart + (if (cachedBlocks.nonEmpty) storageTimeoutMs else idleTimeoutMs)
      } else {
        Long.MaxValue
      }

      timeoutAt = newDeadline

      // If the executor was thought to be timed out, but the new deadline is later than the
      // old one, ask the EAM thread to update the list of timed out executors.
      if (newDeadline > oldDeadline && timedOut) {
        nextTimeout.set(Long.MinValue)
      } else {
        updateNextTimeout(newDeadline)
      }
    }
  }
}
