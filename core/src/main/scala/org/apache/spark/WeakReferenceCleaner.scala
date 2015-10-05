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

import java.lang.ref.ReferenceQueue

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}

import org.apache.spark.util.cleanup.{CleanupTask, CleanupTaskWeakReference}

/**
 * Utility trait that keeps a long running thread for cleaning up weak references
 * after they are GCed. Currently implemented by ContextCleaner and ExecutorCleaner
 * only.
 */
private[spark] trait WeakReferenceCleaner extends Logging {

  private val referenceBuffer = new ArrayBuffer[CleanupTaskWeakReference]
    with SynchronizedBuffer[CleanupTaskWeakReference]

  private val referenceQueue = new ReferenceQueue[AnyRef]

  private val cleaningThread = new Thread() { override def run() { keepCleaning() }}

  private var stopped = false

  /** Start the cleaner. */
  def start(): Unit = {
    cleaningThread.setDaemon(true)
    cleaningThread.setName(cleanupThreadName())
    cleaningThread.start()
  }

  def stop(): Unit = {
    stopped = true
    synchronized {
      // Interrupt the cleaning thread, but wait until the current task has finished before
      // doing so. This guards against the race condition where a cleaning thread may
      // potentially clean similarly named variables created by a different SparkContext,
      // resulting in otherwise inexplicable block-not-found exceptions (SPARK-6132).
      cleaningThread.interrupt()
    }
    cleaningThread.join()
  }

  protected def keepCleaning(): Unit = {
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(WeakReferenceCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        // Synchronize here to avoid being interrupted on stop()
        synchronized {
          reference.map(_.task).foreach { task =>
            logDebug("Got cleaning task " + task)
            referenceBuffer -= reference.get
            handleCleanupForSpecificTask(task)
          }
        }
      } catch {
        case ie: InterruptedException if stopped => // ignore
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
  }

  /** Register an object for cleanup. */
  protected def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
    referenceBuffer += new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue)
  }

  protected def handleCleanupForSpecificTask(task: CleanupTask)
  protected def cleanupThreadName(): String
}

private object WeakReferenceCleaner {
  private val REF_QUEUE_POLL_TIMEOUT = 100
}
