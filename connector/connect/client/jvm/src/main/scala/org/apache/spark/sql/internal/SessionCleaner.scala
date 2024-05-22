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

package org.apache.spark.sql.internal

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * Classes that represent cleaning tasks.
 */
private sealed trait CleanupTask
private case class CleanupCachedRemoteRelation(dfID: String) extends CleanupTask

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
 * An asynchronous cleaner for objects.
 *
 * This maintains a weak reference for each CashRemoteRelation, etc. of interest, to be processed
 * when the associated object goes out of scope of the application. Actual cleanup is performed in
 * a separate daemon thread.
 */
private[sql] class SessionCleaner(session: SparkSession) extends Logging {

  /**
   * How often (seconds) to trigger a garbage collection in this JVM. This context cleaner
   * triggers cleanups only when weak references are garbage collected. In long-running
   * applications with large driver JVMs, where there is little memory pressure on the driver,
   * this may happen very occasionally or not at all. Not cleaning at all may lead to executors
   * running out of disk space after a while.
   */
  private val refQueuePollTimeout: Long = 100

  /**
   * A buffer to ensure that `CleanupTaskWeakReference`s are not garbage collected as long as they
   * have not been handled by the reference queue.
   */
  private val referenceBuffer =
    Collections.newSetFromMap[CleanupTaskWeakReference](new ConcurrentHashMap)

  private val referenceQueue = new ReferenceQueue[AnyRef]

  private val cleaningThread = new Thread() { override def run(): Unit = keepCleaning() }

  @volatile private var started = false
  @volatile private var stopped = false

  /** Start the cleaner. */
  def start(): Unit = {
    cleaningThread.setDaemon(true)
    cleaningThread.setName("Spark Connect Context Cleaner")
    cleaningThread.start()
  }

  /**
   * Stop the cleaning thread and wait until the thread has finished running its current task.
   */
  def stop(): Unit = {
    stopped = true
    // Interrupt the cleaning thread, but wait until the current task has finished before
    // doing so. This guards against the race condition where a cleaning thread may
    // potentially clean similarly named variables created by a different SparkSession.
    synchronized {
      cleaningThread.interrupt()
    }
    cleaningThread.join()
  }

  /** Register a CachedRemoteRelation for cleanup when it is garbage collected. */
  def registerCachedRemoteRelationForCleanup(relation: proto.CachedRemoteRelation): Unit = {
    registerForCleanup(relation, CleanupCachedRemoteRelation(relation.getRelationId))
  }

  /** Register an object for cleanup. */
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
    if (!started) {
      // Lazily starts when the first cleanup is registered.
      start()
      started = true
    }
    referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue))
  }

  /** Keep cleaning objects. */
  private def keepCleaning(): Unit = {
    while (!stopped && !session.client.channel.isShutdown) {
      try {
        val reference = Option(referenceQueue.remove(refQueuePollTimeout))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        // Synchronize here to avoid being interrupted on stop()
        synchronized {
          reference.foreach { ref =>
            logDebug("Got cleaning task " + ref.task)
            referenceBuffer.remove(ref)
            ref.task match {
              case CleanupCachedRemoteRelation(dfID) =>
                doCleanupCachedRemoteRelation(dfID)
            }
          }
        }
      } catch {
        case e: Throwable => logError("Error in cleaning thread", e)
      }
    }
  }

  /** Perform CleanupCachedRemoteRelation cleanup. */
  private[spark] def doCleanupCachedRemoteRelation(dfID: String): Unit = {
    session.execute {
      session.newCommand { builder =>
        builder.getRemoveCachedRemoteRelationCommandBuilder
          .setRelation(proto.CachedRemoteRelation.newBuilder().setRelationId(dfID).build())
      }
    }
  }
}
