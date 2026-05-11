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
package org.apache.spark.udf.worker.core

import java.util.HashMap
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.UDFWorkerSpecification

/**
 * :: Experimental ::
 * Manages [[WorkerDispatcher]] instances, caching them by
 * [[UDFWorkerSpecification]] (protobuf value equality).
 *
 * Callers obtain a dispatcher via [[getDispatcher]] and create
 * sessions on it directly. On [[stop]], all cached dispatchers
 * are closed -- dispatchers are responsible for cleaning up
 * their own sessions.
 *
 * Thread safety: a [[ReentrantReadWriteLock]] allows concurrent
 * [[getDispatcher]] calls (read lock) while [[stop]] has
 * exclusive access (write lock).
 */
@Experimental
class UDFDispatcherManager(
    private val dispatcherFactory: UDFDispatcherFactory,
    workerLogger: WorkerLogger = WorkerLogger.NoOp
) {

  // Guarded by `rwLock`. The read lock is used by getDispatcher
  // (with upgrade when a new dispatcher must be added) and the
  // write lock is used by stop.
  private val rwLock = new ReentrantReadWriteLock()
  private val dispatchers =
    new HashMap[UDFWorkerSpecification, WorkerDispatcher]()
  private var closed = false

  /**
   * Returns the [[WorkerDispatcher]] for the given spec, creating
   * one via the [[UDFDispatcherFactory]] if none exists yet.
   */
  def getDispatcher(
      workerSpec: UDFWorkerSpecification): WorkerDispatcher = {
    // First, try to read an existing dispatcher = quick path
    rwLock.readLock().lock()
    try {
      if (closed) throwClosed()

      // Reading existing dispatcher = quick path
      val dispatcher = dispatchers.get(workerSpec)
      if (dispatcher != null) {
        return dispatcher
      }
    } finally {
      rwLock.readLock().unlock()
    }

    // We need to acquire a new dispatcher
    // = slower path with global lock
    rwLock.writeLock().lock()
    try {
      if (closed) throwClosed()
      // Re-check after acquiring write lock.
      var dispatcher = dispatchers.get(workerSpec)
      if (dispatcher == null) {
        dispatcher = dispatcherFactory.createDispatcher(
          workerSpec, workerLogger)
        workerLogger.info(
          s"Created new dispatcher")
        dispatchers.put(workerSpec, dispatcher)
      }
      dispatcher
    } finally {
      rwLock.writeLock().unlock()
    }
  }

  private def throwClosed(): Nothing =
    throw new IllegalStateException("UDFDispatcherManager is stopped")

  /**
   * Closes all cached dispatchers and resets internal state.
   * Dispatchers are responsible for cleaning up their own
   * sessions.
   */
  def close(): Unit = {
    rwLock.writeLock().lock()
    try {
      if (closed) return
      closed = true
      workerLogger.info(
        "UDFDispatcherManager closing" +
        s" (${dispatchers.size()} dispatchers)")
      dispatchers.forEach { (_, dispatcher) =>
        dispatcher.close()
      }
      dispatchers.clear()
      workerLogger.info("UDFDispatcherManager closed")
    } finally {
      rwLock.writeLock().unlock()
    }
  }
}
