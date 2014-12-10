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

package org.apache.spark.streaming

import java.util.concurrent.{TimeoutException, TimeUnit}
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy

private[streaming] class ContextWaiter {

  private val lock = new ReentrantLock()
  private val condition = lock.newCondition()

  @GuardedBy("lock")
  private var error: Throwable = null

  @GuardedBy("lock")
  private var stopped: Boolean = false

  def notifyError(e: Throwable) = {
    lock.lock()
    try {
      error = e
      condition.signalAll()
    } finally {
      lock.unlock()
    }
  }

  def notifyStop() = {
    lock.lock()
    try {
      stopped = true
      condition.signalAll()
    } finally {
      lock.unlock()
    }
  }

  /**
   * Return `true` if it's stopped; or throw the reported error if `notifyError` has been called; or
   * `false` if the waiting time detectably elapsed before return from the method.
   */
  def waitForStopOrError(timeout: Long = -1): Boolean = {
    lock.lock()
    try {
      if (timeout < 0) {
        while (true) {
          // If already stopped, then exit
          if (stopped) return true
          // If already had error, then throw it
          if (error != null) throw error

          condition.await()
        }
      } else {
        var nanos = TimeUnit.MILLISECONDS.toNanos(timeout)
        while (true) {
          // If already stopped, then exit
          if (stopped) return true
          // If already had error, then throw it
          if (error != null) throw error
          // If no time remains, then exit
          if (nanos <= 0) return false

          nanos = condition.awaitNanos(nanos)
        }
      }
      // Never reached. Make the compiler happy.
      true
    } finally {
      lock.unlock()
    }
  }
}
