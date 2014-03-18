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

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import org.apache.spark.storage.StorageLevel

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
  private sealed trait CleaningTask
  private case class CleanRDD(rddId: Int) extends CleaningTask
  private case class CleanShuffle(shuffleId: Int) extends CleaningTask
  // TODO: add CleanBroadcast

  private val queue = new LinkedBlockingQueue[CleaningTask]

  protected val listeners = new ArrayBuffer[CleanerListener]
    with SynchronizedBuffer[CleanerListener]

  private val cleaningThread = new Thread() { override def run() { keepCleaning() }}

  @volatile private var stopped = false

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
   * Schedule cleanup of RDD data. Do not perform any time or resource intensive
   * computation in this function as this is called from a finalize() function.
   */
  def scheduleRDDCleanup(rddId: Int) {
    enqueue(CleanRDD(rddId))
    logDebug("Enqueued RDD " + rddId + " for cleaning up")
  }

  /**
   * Schedule cleanup of shuffle data. Do not perform any time or resource intensive
   * computation in this function as this is called from a finalize() function.
   */
  def scheduleShuffleCleanup(shuffleId: Int) {
    enqueue(CleanShuffle(shuffleId))
    logDebug("Enqueued shuffle " + shuffleId + " for cleaning up")
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

  /**
   * Enqueue a cleaning task. Do not perform any time or resource intensive
   * computation in this function as this is called from a finalize() function.
   */
  private def enqueue(task: CleaningTask) {
    queue.put(task)
  }

  /** Keep cleaning RDDs and shuffle data */
  private def keepCleaning() {
    while (!isStopped) {
      try {
        val taskOpt = Option(queue.poll(100, TimeUnit.MILLISECONDS))
        taskOpt.foreach { task =>
          logDebug("Got cleaning task " + taskOpt.get)
          task match {
            case CleanRDD(rddId) => doCleanRDD(rddId)
            case CleanShuffle(shuffleId) => doCleanShuffle(shuffleId)
          }
        }
      } catch {
        case ie: InterruptedException =>
          if (!isStopped) logWarning("Cleaning thread interrupted")
        case t: Throwable => logError("Error in cleaning thread", t)
      }
    }
  }

  /** Perform RDD cleaning */
  private def doCleanRDD(rddId: Int) {
    try {
      logDebug("Cleaning RDD " + rddId)
      unpersistRDD(rddId, false)
      listeners.foreach(_.rddCleaned(rddId))
      logInfo("Cleaned RDD " + rddId)
    } catch {
      case t: Throwable => logError("Error cleaning RDD " + rddId, t)
    }
  }

  /** Perform shuffle cleaning */
  private def doCleanShuffle(shuffleId: Int) {
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

  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  private def blockManagerMaster = sc.env.blockManager.master

  private def isStopped = stopped
}
