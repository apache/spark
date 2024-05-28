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

package org.apache.spark

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{ACCUMULATOR_ID, BROADCAST_ID, LISTENER, RDD_ID, SHUFFLE_ID}
import org.apache.spark.internal.config._
import org.apache.spark.rdd.{RDD, ReliableRDDCheckpointData}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, ThreadUtils, Utils}

/**
 * Classes that represent cleaning tasks.
 */
private sealed trait CleanupTask
private case class CleanRDD(rddId: Int) extends CleanupTask
private case class CleanShuffle(shuffleId: Int) extends CleanupTask
private case class CleanBroadcast(broadcastId: Long) extends CleanupTask
private case class CleanAccum(accId: Long) extends CleanupTask
private case class CleanCheckpoint(rddId: Int) extends CleanupTask
private case class CleanSparkListener(listener: SparkListener) extends CleanupTask

/**
 * A WeakReference associated with a CleanupTask.
 *
 * When the referent object becomes only weakly reachable, the corresponding
 * CleanupTaskWeakReference is automatically added to the given reference queue.
 */
private class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
  extends WeakReference(referent, referenceQueue)

/**
 * An asynchronous cleaner for RDD, shuffle, and broadcast state.
 *
 * This maintains a weak reference for each RDD, ShuffleDependency, and Broadcast of interest,
 * to be processed when the associated object goes out of scope of the application. Actual
 * cleanup is performed in a separate daemon thread.
 */
private[spark] class ContextCleaner(
    sc: SparkContext,
    shuffleDriverComponents: ShuffleDriverComponents) extends Logging {

  /**
   * A buffer to ensure that `CleanupTaskWeakReference`s are not garbage collected as long as they
   * have not been handled by the reference queue.
   */
  private val referenceBuffer =
    Collections.newSetFromMap[CleanupTaskWeakReference](new ConcurrentHashMap)

  private val referenceQueue = new ReferenceQueue[AnyRef]

  private val listeners = new ConcurrentLinkedQueue[CleanerListener]()

  private val cleaningThread = new Thread() { override def run(): Unit = keepCleaning() }

  private val periodicGCService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("context-cleaner-periodic-gc")

  /**
   * How often to trigger a garbage collection in this JVM.
   *
   * This context cleaner triggers cleanups only when weak references are garbage collected.
   * In long-running applications with large driver JVMs, where there is little memory pressure
   * on the driver, this may happen very occasionally or not at all. Not cleaning at all may
   * lead to executors running out of disk space after a while.
   */
  private val periodicGCInterval = sc.conf.get(CLEANER_PERIODIC_GC_INTERVAL)

  /**
   * Whether the cleaning thread will block on cleanup tasks (other than shuffle, which
   * is controlled by the `spark.cleaner.referenceTracking.blocking.shuffle` parameter).
   *
   * Due to SPARK-3015, this is set to true by default. This is intended to be only a temporary
   * workaround for the issue, which is ultimately caused by the way the BlockManager endpoints
   * issue inter-dependent blocking RPC messages to each other at high frequencies. This happens,
   * for instance, when the driver performs a GC and cleans up all broadcast blocks that are no
   * longer in scope.
   */
  private val blockOnCleanupTasks = sc.conf.get(CLEANER_REFERENCE_TRACKING_BLOCKING)

  /**
   * Whether the cleaning thread will block on shuffle cleanup tasks.
   *
   * When context cleaner is configured to block on every delete request, it can throw timeout
   * exceptions on cleanup of shuffle blocks, as reported in SPARK-3139. To avoid that, this
   * parameter by default disables blocking on shuffle cleanups. Note that this does not affect
   * the cleanup of RDDs and broadcasts. This is intended to be a temporary workaround,
   * until the real RPC issue (referred to in the comment above `blockOnCleanupTasks`) is
   * resolved.
   */
  private val blockOnShuffleCleanupTasks =
    sc.conf.get(CLEANER_REFERENCE_TRACKING_BLOCKING_SHUFFLE)

  @volatile private var stopped = false

  /** Attach a listener object to get information of when objects are cleaned. */
  def attachListener(listener: CleanerListener): Unit = {
    listeners.add(listener)
  }

  /** Start the cleaner. */
  def start(): Unit = {
    cleaningThread.setDaemon(true)
    cleaningThread.setName("Spark Context Cleaner")
    cleaningThread.start()
    periodicGCService.scheduleAtFixedRate(() => System.gc(),
      periodicGCInterval, periodicGCInterval, TimeUnit.SECONDS)
  }

  /**
   * Stop the cleaning thread and wait until the thread has finished running its current task.
   */
  def stop(): Unit = {
    stopped = true
    // Interrupt the cleaning thread, but wait until the current task has finished before
    // doing so. This guards against the race condition where a cleaning thread may
    // potentially clean similarly named variables created by a different SparkContext,
    // resulting in otherwise inexplicable block-not-found exceptions (SPARK-6132).
    synchronized {
      cleaningThread.interrupt()
    }
    cleaningThread.join()
    periodicGCService.shutdown()
  }

  /** Register an RDD for cleanup when it is garbage collected. */
  def registerRDDForCleanup(rdd: RDD[_]): Unit = {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  def registerAccumulatorForCleanup(a: AccumulatorV2[_, _]): Unit = {
    registerForCleanup(a, CleanAccum(a.id))
  }

  /** Register a ShuffleDependency for cleanup when it is garbage collected. */
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]): Unit = {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /** Register a Broadcast for cleanup when it is garbage collected. */
  def registerBroadcastForCleanup[T](broadcast: Broadcast[T]): Unit = {
    registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
  }

  /** Register a RDDCheckpointData for cleanup when it is garbage collected. */
  def registerRDDCheckpointDataForCleanup[T](rdd: RDD[_], parentId: Int): Unit = {
    registerForCleanup(rdd, CleanCheckpoint(parentId))
  }

  /** Register a SparkListener to be cleaned up when its owner is garbage collected. */
  def registerSparkListenerForCleanup(
      listenerOwner: AnyRef,
      listener: SparkListener): Unit = {
    registerForCleanup(listenerOwner, CleanSparkListener(listener))
  }

  /** Register an object for cleanup. */
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
    referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue))
  }

  /** Keep cleaning RDD, shuffle, and broadcast state. */
  private def keepCleaning(): Unit = Utils.tryOrStopSparkContext(sc) {
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        // Synchronize here to avoid being interrupted on stop()
        synchronized {
          reference.foreach { ref =>
            logDebug("Got cleaning task " + ref.task)
            referenceBuffer.remove(ref)
            ref.task match {
              case CleanRDD(rddId) =>
                doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
              case CleanShuffle(shuffleId) =>
                doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
              case CleanBroadcast(broadcastId) =>
                doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
              case CleanAccum(accId) =>
                doCleanupAccum(accId, blocking = blockOnCleanupTasks)
              case CleanCheckpoint(rddId) =>
                doCleanCheckpoint(rddId)
              case CleanSparkListener(listener) =>
                doCleanSparkListener(listener)
            }
          }
        }
      } catch {
        case ie: InterruptedException if stopped => // ignore
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
  }

  /** Perform RDD cleanup. */
  def doCleanupRDD(rddId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning RDD " + rddId)
      sc.unpersistRDD(rddId, blocking)
      listeners.asScala.foreach(_.rddCleaned(rddId))
      logDebug("Cleaned RDD " + rddId)
    } catch {
      case e: Exception => logError(log"Error cleaning RDD ${MDC(RDD_ID, rddId)}", e)
    }
  }

  /** Perform shuffle cleanup. */
  def doCleanupShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    try {
      if (mapOutputTrackerMaster.containsShuffle(shuffleId)) {
        logDebug("Cleaning shuffle " + shuffleId)
        // Shuffle must be removed before it's unregistered from the output tracker
        // to find blocks served by the shuffle service on deallocated executors
        shuffleDriverComponents.removeShuffle(shuffleId, blocking)
        mapOutputTrackerMaster.unregisterShuffle(shuffleId)
        listeners.asScala.foreach(_.shuffleCleaned(shuffleId))
        logDebug("Cleaned shuffle " + shuffleId)
      } else {
        logDebug("Asked to cleanup non-existent shuffle (maybe it was already removed)")
      }
    } catch {
      case e: Exception => logError(log"Error cleaning shuffle ${MDC(SHUFFLE_ID, shuffleId)}", e)
    }
  }

  /** Perform broadcast cleanup. */
  def doCleanupBroadcast(broadcastId: Long, blocking: Boolean): Unit = {
    try {
      logDebug(s"Cleaning broadcast $broadcastId")
      broadcastManager.unbroadcast(broadcastId, true, blocking)
      listeners.asScala.foreach(_.broadcastCleaned(broadcastId))
      logDebug(s"Cleaned broadcast $broadcastId")
    } catch {
      case e: Exception =>
        logError(log"Error cleaning broadcast ${MDC(BROADCAST_ID, broadcastId)}", e)
    }
  }

  /** Perform accumulator cleanup. */
  def doCleanupAccum(accId: Long, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning accumulator " + accId)
      AccumulatorContext.remove(accId)
      listeners.asScala.foreach(_.accumCleaned(accId))
      logDebug("Cleaned accumulator " + accId)
    } catch {
      case e: Exception =>
        logError(log"Error cleaning accumulator ${MDC(ACCUMULATOR_ID, accId)}", e)
    }
  }

  /**
   * Clean up checkpoint files written to a reliable storage.
   * Locally checkpointed files are cleaned up separately through RDD cleanups.
   */
  def doCleanCheckpoint(rddId: Int): Unit = {
    try {
      logDebug("Cleaning rdd checkpoint data " + rddId)
      ReliableRDDCheckpointData.cleanCheckpoint(sc, rddId)
      listeners.asScala.foreach(_.checkpointCleaned(rddId))
      logDebug("Cleaned rdd checkpoint data " + rddId)
    }
    catch {
      case e: Exception =>
        logError(log"Error cleaning rdd checkpoint data ${MDC(RDD_ID, rddId)}", e)
    }
  }

  def doCleanSparkListener(listener: SparkListener): Unit = {
    try {
      logDebug(s"Cleaning Spark listener $listener")
      sc.listenerBus.removeListener(listener)
      logDebug(s"Cleaned Spark listener $listener")
    } catch {
      case e: Exception =>
        logError(log"Error cleaning Spark listener ${MDC(LISTENER, listener)}", e)
    }
  }

  private def broadcastManager = sc.env.broadcastManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}

private object ContextCleaner {
  private val REF_QUEUE_POLL_TIMEOUT = 100
}

/**
 * Listener class used when any item has been cleaned by the Cleaner class.
 */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int): Unit
  def shuffleCleaned(shuffleId: Int): Unit
  def broadcastCleaned(broadcastId: Long): Unit
  def accumCleaned(accId: Long): Unit
  def checkpointCleaned(rddId: Long): Unit
}
