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

package org.apache.spark.sql

import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.ListBuffer

import org.apache.spark.Logging
import org.apache.spark.sql.execution.QueryExecution


/**
 * The interface of query execution listener that can be used to analyze execution metrics.
 *
 * Note that implementations should guarantee thread-safety as they will be used in a non
 * thread-safe way.
 */
trait QueryExecutionListener {
  def onSuccess(funcName: String, qe: QueryExecution, duration: Long)

  def onFailure(funcName: String, qe: QueryExecution, exception: Exception)
}

class ExecutionListenerManager extends Logging {
  private[this] val listeners = ListBuffer.empty[QueryExecutionListener]
  private[this] val lock = new ReentrantReadWriteLock()

  /** Acquires a read lock on the cache for the duration of `f`. */
  private def readLock[A](f: => A): A = {
    val rl = lock.readLock()
    rl.lock()
    try f finally {
      rl.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`. */
  private def writeLock[A](f: => A): A = {
    val wl = lock.writeLock()
    wl.lock()
    try f finally {
      wl.unlock()
    }
  }

  def register(listener: QueryExecutionListener): Unit = writeLock {
    listeners += listener
  }

  def unregister(listener: QueryExecutionListener): Unit = writeLock {
    listeners -= listener
  }

  def clear(): Unit = writeLock {
    listeners.clear()
  }

  private[sql] def onSuccess(
      funcName: String,
      qe: QueryExecution,
      duration: Long): Unit = readLock {
    withErrorHandling { listener =>
      listener.onSuccess(funcName, qe, duration)
    }
  }

  private[sql] def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception): Unit = readLock {
    withErrorHandling { listener =>
      listener.onFailure(funcName, qe, exception)
    }
  }

  private def withErrorHandling(f: QueryExecutionListener => Unit): Unit = {
    for (listener <- listeners) {
      try {
        f(listener)
      } catch {
        case e: Exception => logWarning("error executing query execution listener", e)
      }
    }
  }
}
