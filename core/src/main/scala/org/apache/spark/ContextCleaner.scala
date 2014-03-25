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

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}

import org.apache.spark.rdd.RDD

/** Listener class used for testing when any item has been cleaned by the Cleaner class */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int)
  def shuffleCleaned(shuffleId: Int)
}

/**
 * Cleans RDDs and shuffle data.
 */
private[spark] class ContextCleaner(sc: SparkContext) extends Logging {

  /** Classes to represent cleaning tasks */
  private sealed trait CleanupTask
  private case class CleanRDD(rddId: Int) extends CleanupTask
  private case class CleanShuffle(shuffleId: Int) extends CleanupTask
  // TODO: add CleanBroadcast

  private val referenceBuffer = new ArrayBuffer[WeakReferenceWithCleanupTask]
    with SynchronizedBuffer[WeakReferenceWithCleanupTask]
  private val referenceQueue = new ReferenceQueue[AnyRef]

  private val listeners = new ArrayBuffer[CleanerListener]
    with SynchronizedBuffer[CleanerListener]

  private val cleaningThread = new Thread() { override def run() { keepCleaning() }}

  private val REF_QUEUE_POLL_TIMEOUT = 100

  @volatile private var stopped = false

  private class WeakReferenceWithCleanupTask(referent: AnyRef, val task: CleanupTask)
    extends WeakReference(referent, referenceQueue)

  /** Start the cleaner */
  def start() {
    cleaningThread.setDaemon(true)
    cleaningThread.setName("ContextCleaner")
    cleaningThread.start()
  }

  /** Stop the cleaner */
  def stop() {
    stopped = true
    cleaningThread.interrupt()
  }

  /**
   * Register a RDD for cleanup when it is garbage collected.
   */
  def registerRDDForCleanup(rdd: RDD[_]) {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  /**
   * Register a shuffle dependency for cleanup when it is garbage collected.
   */
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _]) {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /** Cleanup RDD. */
  def cleanupRDD(rdd: RDD[_]) {
    doCleanupRDD(rdd.id)
  }

  /** Cleanup shuffle. */
  def cleanupShuffle(shuffleDependency: ShuffleDependency[_, _]) {
    doCleanupShuffle(shuffleDependency.shuffleId)
  }

  /** Attach a listener object to get information of when objects are cleaned. */
  def attachListener(listener: CleanerListener) {
    listeners += listener
  }

  /** Unpersists RDD and remove all blocks for it from memory and disk. */
  def unpersistRDD(rddId: Int, blocking: Boolean) {
    logDebug("Unpersisted RDD " + rddId)
    sc.env.blockManager.master.removeRdd(rddId, blocking)
    sc.persistentRdds.remove(rddId)
  }

  /** Register an object for cleanup. */
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask) {
    referenceBuffer += new WeakReferenceWithCleanupTask(objectForCleanup, task)
  }

  /** Keep cleaning RDDs and shuffle data */
  private def keepCleaning() {
    while (!isStopped) {
      try {
        val reference = Option(referenceQueue.remove(REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[WeakReferenceWithCleanupTask])
        reference.map(_.task).foreach { task =>
          logDebug("Got cleaning task " + task)
          referenceBuffer -= reference.get
          task match {
            case CleanRDD(rddId) => doCleanupRDD(rddId)
            case CleanShuffle(shuffleId) => doCleanupShuffle(shuffleId)
          }
        }
      } catch {
        case ie: InterruptedException =>
          if (!isStopped) logWarning("Cleaning thread interrupted")
        case t: Throwable => logError("Error in cleaning thread", t)
      }
    }
  }

  /** Perform RDD cleanup. */
  private def doCleanupRDD(rddId: Int) {
    try {
      logDebug("Cleaning RDD " + rddId)
      unpersistRDD(rddId, false)
      listeners.foreach(_.rddCleaned(rddId))
      logInfo("Cleaned RDD " + rddId)
    } catch {
      case t: Throwable => logError("Error cleaning RDD " + rddId, t)
    }
  }

  /** Perform shuffle cleanup. */
  private def doCleanupShuffle(shuffleId: Int) {
    try {
      logDebug("Cleaning shuffle " + shuffleId)
      mapOutputTrackerMaster.unregisterShuffle(shuffleId)
      blockManagerMaster.removeShuffle(shuffleId)
      listeners.foreach(_.shuffleCleaned(shuffleId))
      logInfo("Cleaned shuffle " + shuffleId)
    } catch {
      case t: Throwable => logError("Error cleaning shuffle " + shuffleId, t)
    }
  }

  private def mapOutputTrackerMaster =
    sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  private def blockManagerMaster = sc.env.blockManager.master

  private def isStopped = stopped
}
