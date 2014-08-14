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
import java.lang.reflect.Field

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * Classes that represent cleaning tasks.
 */
private sealed trait CleanupTask
private case class CleanRDD(rddId: Int) extends CleanupTask
private case class CleanShuffle(shuffleId: Int) extends CleanupTask
private case class CleanBroadcast(broadcastId: Long) extends CleanupTask

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
private[spark] class ContextCleaner(sc: SparkContext) extends Logging {

  private val referenceBuffer = new ArrayBuffer[CleanupTaskWeakReference]
    with SynchronizedBuffer[CleanupTaskWeakReference]

  private val referenceQueue = new ReferenceQueue[AnyRef]

  private val listeners = new ArrayBuffer[CleanerListener]
    with SynchronizedBuffer[CleanerListener]

  private val cleaningThread = new Thread() { override def run() { keepCleaning() }}

  /**
   * Keep track of the reference queue length and log an error if this exceeds a certain capacity.
   * Unfortunately, Java's ReferenceQueue exposes neither the queue length nor the enqueue method,
   * so we have to do this through reflection. This is expensive, however, so we should access
   * this field only once in a while.
   */
  private val queueCapacity = 10000
  private var queueFullErrorMessageLogged = false
  private val queueLengthAccessor: Option[Field] = {
    try {
      val f = classOf[ReferenceQueue[AnyRef]].getDeclaredField("queueLength")
      f.setAccessible(true)
      Some(f)
    } catch {
      case e: Exception =>
        logDebug("Failed to expose java.lang.ref.ReferenceQueue's queueLength field: " + e)
        None
    }
  }
  private val logQueueLengthInterval = 1000

  /**
   * Whether the cleaning thread will block on cleanup tasks.
   *
   * Due to SPARK-3015, this is set to true by default. This is intended to be only a temporary
   * workaround for the issue, which is ultimately caused by the way the BlockManager actors
   * issue inter-dependent blocking Akka messages to each other at high frequencies. This happens,
   * for instance, when the driver performs a GC and cleans up all broadcast blocks that are no
   * longer in scope.
   */
  private val blockOnCleanupTasks = sc.conf.getBoolean(
    "spark.cleaner.referenceTracking.blocking", true)

  @volatile private var stopped = false

  /** Attach a listener object to get information of when objects are cleaned. */
  def attachListener(listener: CleanerListener) {
    listeners += listener
  }

  /** Start the cleaner. */
  def start() {
    cleaningThread.setDaemon(true)
    cleaningThread.setName("Spark Context Cleaner")
    cleaningThread.start()
  }

  /** Stop the cleaner. */
  def stop() {
    stopped = true
  }

  /** Register a RDD for cleanup when it is garbage collected. */
  def registerRDDForCleanup(rdd: RDD[_]) {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  /** Register a ShuffleDependency for cleanup when it is garbage collected. */
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]) {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /** Register a Broadcast for cleanup when it is garbage collected. */
  def registerBroadcastForCleanup[T](broadcast: Broadcast[T]) {
    registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
  }

  /** Register an object for cleanup. */
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask) {
    referenceBuffer += new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue)
  }

  /** Keep cleaning RDD, shuffle, and broadcast state. */
  private def keepCleaning(): Unit = Utils.logUncaughtExceptions {
    var iteration = 0
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        reference.map(_.task).foreach { task =>
          logDebug("Got cleaning task " + task)
          referenceBuffer -= reference.get
          task match {
            case CleanRDD(rddId) =>
              doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
            case CleanShuffle(shuffleId) =>
              doCleanupShuffle(shuffleId, blocking = blockOnCleanupTasks)
            case CleanBroadcast(broadcastId) =>
              doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
          }
          if (iteration % logQueueLengthInterval == 0) {
            logQueueLength()
          }
        }
      } catch {
        case e: Exception => logError("Error in cleaning thread", e)
      }
      iteration += 1
    }
  }

  /** Perform RDD cleanup. */
  def doCleanupRDD(rddId: Int, blocking: Boolean) {
    try {
      logDebug("Cleaning RDD " + rddId)
      sc.unpersistRDD(rddId, blocking)
      listeners.foreach(_.rddCleaned(rddId))
      logInfo("Cleaned RDD " + rddId)
    } catch {
      case e: Exception => logError("Error cleaning RDD " + rddId, e)
    }
  }

  /** Perform shuffle cleanup, asynchronously. */
  def doCleanupShuffle(shuffleId: Int, blocking: Boolean) {
    try {
      logDebug("Cleaning shuffle " + shuffleId)
      mapOutputTrackerMaster.unregisterShuffle(shuffleId)
      blockManagerMaster.removeShuffle(shuffleId, blocking)
      listeners.foreach(_.shuffleCleaned(shuffleId))
      logInfo("Cleaned shuffle " + shuffleId)
    } catch {
      case e: Exception => logError("Error cleaning shuffle " + shuffleId, e)
    }
  }

  /** Perform broadcast cleanup. */
  def doCleanupBroadcast(broadcastId: Long, blocking: Boolean) {
    try {
      logDebug("Cleaning broadcast " + broadcastId)
      broadcastManager.unbroadcast(broadcastId, true, blocking)
      listeners.foreach(_.broadcastCleaned(broadcastId))
      logInfo("Cleaned broadcast " + broadcastId)
    } catch {
      case e: Exception => logError("Error cleaning broadcast " + broadcastId, e)
    }
  }

  /**
   * Log the length of the reference queue through reflection.
   * This is an expensive operation and should be called sparingly.
   */
  private def logQueueLength(): Unit = {
    try {
      queueLengthAccessor.foreach { field =>
        val length = field.getLong(referenceQueue)
        logDebug("Reference queue size is " + length)
        if (length > queueCapacity) {
          logQueueFullErrorMessage()
        }
      }
    } catch {
      case e: Exception =>
        logDebug("Failed to access reference queue's length through reflection: " + e)
    }
  }

  /**
   * Log an error message to indicate that the queue has exceeded its capacity. Do this only once.
   */
  private def logQueueFullErrorMessage(): Unit = {
    if (!queueFullErrorMessageLogged) {
      queueFullErrorMessageLogged = true
      logError(s"Reference queue size in ContextCleaner has exceeded $queueCapacity! " +
        "This means the rate at which we clean up RDDs, shuffles, and/or broadcasts is too slow.")
      if (blockOnCleanupTasks) {
        logError("Consider setting spark.cleaner.referenceTracking.blocking to false." +
          "Note that there is a known issue (SPARK-3015) in disabling blocking, especially if " +
          "the workload involves creating many RDDs in quick successions.")
      }
    }
  }

  private def blockManagerMaster = sc.env.blockManager.master
  private def broadcastManager = sc.env.broadcastManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}

private object ContextCleaner {
  private val REF_QUEUE_POLL_TIMEOUT = 100
}

/**
 * Listener class used for testing when any item has been cleaned by the Cleaner class.
 */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int)
  def shuffleCleaned(shuffleId: Int)
  def broadcastCleaned(broadcastId: Long)
}
