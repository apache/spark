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

  def this(name: String) = {
    this(null, name)
  }

  private class UninterruptibleLock {
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
     * Indicates that we should wait for interrupt() call before proceeding.
     */
    @GuardedBy("uninterruptibleLock")
    private var awaitInterruptThread = false

    /**
     * Set [[uninterruptible]] to given value and returns the previous value.
     */
    def getAndSetUninterruptible(value: Boolean): Boolean = synchronized {
      val uninterruptible = this.uninterruptible
      this.uninterruptible = value
      uninterruptible
    }

    def setShouldInterruptThread(value: Boolean): Unit = synchronized {
      shouldInterruptThread = value
    }

    def setAwaitInterruptThread(value: Boolean): Unit = synchronized {
      awaitInterruptThread = value
    }

    /**
     * Is call to [[java.lang.Thread.interrupt()]] pending
     */
    def isInterruptPending: Boolean = synchronized {
      // Clear the interrupted status if it's set.
      shouldInterruptThread = Thread.interrupted() || shouldInterruptThread
      // wait for super.interrupt() to be called
      !shouldInterruptThread && awaitInterruptThread
    }

    /**
     * Set [[uninterruptible]] back to false and call [[java.lang.Thread.interrupt()]] to
     * recover interrupt state if necessary
     */
    def recoverInterrupt(): Unit = synchronized {
      uninterruptible = false
      if (shouldInterruptThread) {
        shouldInterruptThread = false
        // Recover the interrupted status
        UninterruptibleThread.super.interrupt()
      }
    }

    /**
     * Is it safe to call [[java.lang.Thread.interrupt()]] and interrupt the current thread
     * @return true when there is no concurrent [[runUninterruptibly()]] call ([[uninterruptible]]
     *         is true) and no concurrent [[interrupt()]] call, otherwise false
     */
    def isInterruptible: Boolean = synchronized {
      shouldInterruptThread = uninterruptible
      // as we are releasing uninterruptibleLock before calling super.interrupt() there is a
      // possibility that runUninterruptibly() would be called after lock is released but before
      // super.interrupt() is called. In this case to prevent runUninterruptibly() from being
      // interrupted, we use awaitInterruptThread flag. We need to set it only if
      // runUninterruptibly() is not yet set uninterruptible to true (!shouldInterruptThread) and
      // there is no other threads that called interrupt (awaitInterruptThread is already true)
      if (!shouldInterruptThread && !awaitInterruptThread) {
        awaitInterruptThread = true
        true
      } else {
        false
      }
    }
  }

  /** A monitor to protect "uninterruptible" and "interrupted" */
  private val uninterruptibleLock = new UninterruptibleLock

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

    if (uninterruptibleLock.getAndSetUninterruptible(true)) {
      // We are already in the uninterruptible status. So just run "f" and return
      return f
    }

    while (uninterruptibleLock.isInterruptPending) {
      try {
        Thread.sleep(100)
      } catch {
        case _: InterruptedException => uninterruptibleLock.setShouldInterruptThread(true)
      }
    }

    try {
      f
    } finally {
      uninterruptibleLock.recoverInterrupt()
    }
  }

  /**
   * Interrupt `this` thread if possible. If `this` is in the uninterruptible status, it won't be
   * interrupted until it enters into the interruptible status.
   */
  override def interrupt(): Unit = {
    if (uninterruptibleLock.isInterruptible) {
      try {
        super.interrupt()
      } finally {
        uninterruptibleLock.setAwaitInterruptThread(false)
      }
    }
  }
}
