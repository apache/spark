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

package org.apache.spark.util

import javax.annotation.concurrent.GuardedBy

/**
 * A special Thread that provides "runUninterruptibly" to allow running codes without being
 * interrupted by `Thread.interrupt()`. If `Thread.interrupt()` is called during runUninterruptibly
 * is running, it won't set the interrupted status. Instead, setting the interrupted status will be
 * deferred until it's returning from "runUninterruptibly".
 *
 * Note: "runUninterruptibly" should be called only in `this` thread.
 */
private[spark] class UninterruptibleThread(
    target: Runnable,
    name: String) extends Thread(target, name) {

  def this(name: String) {
    this(null, name)
  }

  /** A monitor to protect "uninterruptible" and "interrupted" */
  private val uninterruptibleLock = new Object

  /**
   * Indicates if `this`  thread are in the uninterruptible status. If so, interrupting
   * "this" will be deferred until `this`  enters into the interruptible status.
   */
  @GuardedBy("uninterruptibleLock")
  private var uninterruptible = false

  /**
   * Indicates if we should interrupt `this` when we are leaving the uninterruptible zone.
   */
  @GuardedBy("uninterruptibleLock")
  private var shouldInterruptThread = false

  /**
   * Run `f` uninterruptibly in `this` thread. The thread won't be interrupted before returning
   * from `f`.
   *
   * Note: this method should be called only in `this` thread.
   */
  def runUninterruptibly[T](f: => T): T = {
    if (Thread.currentThread() != this) {
      throw new IllegalStateException(s"Call runUninterruptibly in a wrong thread. " +
        s"Expected: $this but was ${Thread.currentThread()}")
    }

    if (uninterruptibleLock.synchronized { uninterruptible }) {
      // We are already in the uninterruptible status. So just run "f" and return
      return f
    }

    uninterruptibleLock.synchronized {
      // Clear the interrupted status if it's set.
      shouldInterruptThread = Thread.interrupted() || shouldInterruptThread
      uninterruptible = true
    }
    try {
      f
    } finally {
      uninterruptibleLock.synchronized {
        uninterruptible = false
        if (shouldInterruptThread) {
          // Recover the interrupted status
          super.interrupt()
          shouldInterruptThread = false
        }
      }
    }
  }

  /**
   * Interrupt `this` thread if possible. If `this` is in the uninterruptible status, it won't be
   * interrupted until it enters into the interruptible status.
   */
  override def interrupt(): Unit = {
    uninterruptibleLock.synchronized {
      if (uninterruptible) {
        shouldInterruptThread = true
      } else {
        super.interrupt()
      }
    }
  }
}
