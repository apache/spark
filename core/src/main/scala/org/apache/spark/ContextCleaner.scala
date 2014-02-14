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

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

import org.apache.spark.rdd.RDD

/** Listener class used for testing when any item has been cleaned by the Cleaner class */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int)
  def shuffleCleaned(shuffleId: Int)
}

/**
 * Cleans RDDs and shuffle data. This should be instantiated only on the driver.
 */
private[spark] class ContextCleaner(env: SparkEnv) extends Logging {

  /** Classes to represent cleaning tasks */
  private sealed trait CleaningTask
  private case class CleanRDD(sc: SparkContext, id: Int) extends CleaningTask
  private case class CleanShuffle(id: Int) extends CleaningTask
  // TODO: add CleanBroadcast

  private val QUEUE_CAPACITY = 1000
  private val queue = new ArrayBlockingQueue[CleaningTask](QUEUE_CAPACITY)

  protected val listeners = new ArrayBuffer[CleanerListener]
    with SynchronizedBuffer[CleanerListener]

  private val cleaningThread = new Thread() { override def run() { keepCleaning() }}

  private var stopped = false

  /** Start the cleaner */
  def start() {
    cleaningThread.setDaemon(true)
    cleaningThread.start()
  }

  /** Stop the cleaner */
  def stop() {
    synchronized { stopped = true }
    cleaningThread.interrupt()
  }

  /** Clean all data and metadata related to a RDD, including shuffle files and metadata */
  def cleanRDD(rdd: RDD[_]) {
    enqueue(CleanRDD(rdd.sparkContext, rdd.id))
    logDebug("Enqueued RDD " + rdd + " for cleaning up")
  }

  def cleanShuffle(shuffleId: Int) {
    enqueue(CleanShuffle(shuffleId))
    logDebug("Enqueued shuffle " + shuffleId + " for cleaning up")
  }

  def attachListener(listener: CleanerListener) {
    listeners += listener
  }
  /** Enqueue a cleaning task */
  private def enqueue(task: CleaningTask) {
    queue.put(task)
  }

  /** Keep cleaning RDDs and shuffle data */
  private def keepCleaning() {
    try {
      while (!isStopped) {
        val taskOpt = Option(queue.poll(100, TimeUnit.MILLISECONDS))
        if (taskOpt.isDefined) {
          logDebug("Got cleaning task " + taskOpt.get)
          taskOpt.get match {
            case CleanRDD(sc, rddId) => doCleanRDD(sc, rddId)
            case CleanShuffle(shuffleId) => doCleanShuffle(shuffleId)
          }
        }
      }
    } catch {
      case ie: java.lang.InterruptedException =>
        if (!isStopped) logWarning("Cleaning thread interrupted")
    }
  }

  /** Perform RDD cleaning */
  private def doCleanRDD(sc: SparkContext, rddId: Int) {
    logDebug("Cleaning rdd "+ rddId)
    sc.env.blockManager.master.removeRdd(rddId, false)
    sc.persistentRdds.remove(rddId)
    listeners.foreach(_.rddCleaned(rddId))
    logInfo("Cleaned rdd "+ rddId)
  }

  /** Perform shuffle cleaning */
  private def doCleanShuffle(shuffleId: Int) {
    logDebug("Cleaning shuffle "+ shuffleId)
    mapOutputTrackerMaster.unregisterShuffle(shuffleId)
    blockManager.master.removeShuffle(shuffleId)
    listeners.foreach(_.shuffleCleaned(shuffleId))
    logInfo("Cleaned shuffle " + shuffleId)
  }

  private def mapOutputTrackerMaster = env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  private def blockManager = env.blockManager

  private def isStopped = synchronized { stopped }
}