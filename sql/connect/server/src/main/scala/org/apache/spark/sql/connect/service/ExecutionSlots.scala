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

package org.apache.spark.sql.connect.service

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.spark.SparkSQLException
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.util.ThreadUtils

/**
 * Thrown when a queued execution does not get an execution slot within
 * `spark.connect.execute.maxConcurrentQueries.timeout`.
 */
private[connect] class SparkConnectConcurrentExecutionsTimeoutException(
    limit: Int,
    timeout: String)
    extends SparkSQLException(
      errorClass = "CONNECT.EXECUTE_CONCURRENT_LIMIT_TIMEOUT",
      messageParameters = Map("limit" -> limit.toString, "timeout" -> timeout))

/** An execution slot that can be released at most once. */
private[connect] class ExecutionPermit(private val slots: ExecutionSlots) {
  private val released = new AtomicBoolean(false)

  def release(): Unit = {
    if (released.compareAndSet(false, true)) {
      slots.release()
    }
  }
}

/**
 * A request for an execution slot, see [[ExecutionSlots.request]]. Its state is guarded by the
 * lock of the owning [[ExecutionSlots]].
 */
private[connect] class ExecutionSlotRequest private[service] (
    private val slots: ExecutionSlots,
    private[service] val onGranted: ExecutionPermit => Unit) {

  private[service] var state: ExecutionSlotRequest.State = ExecutionSlotRequest.Waiting

  @volatile private[service] var timeoutTask: Option[ScheduledFuture[_]] = None

  /** Whether the request is still waiting in the queue. */
  def isWaiting: Boolean = slots.synchronized(state == ExecutionSlotRequest.Waiting)

  /**
   * Leave the queue if the request is still waiting.
   *
   * @return
   *   true if the request was still waiting and will now never be granted, false if it was
   *   already granted (then the owner of the permit is responsible for releasing it) or
   *   abandoned.
   */
  def abandon(): Boolean = slots.abandon(this)
}

private[connect] object ExecutionSlotRequest {
  sealed trait State
  case object Waiting extends State
  case object Granted extends State
  case object Abandoned extends State
}

/**
 * Bounded pool of execution slots, granted in FIFO order.
 *
 * Callers do not block while waiting for a slot: [[request]] registers a callback that is invoked
 * once a slot is granted, either immediately or later from the thread that releases a slot. A
 * request that is still waiting can be abandoned at any time, e.g. when the operation is
 * interrupted. All state transitions happen under the lock of this object, so a request is either
 * granted or abandoned, never both, and a released slot is never lost.
 */
private[connect] class ExecutionSlots(val limit: Int) extends Logging {
  require(limit > 0, s"The execution slot limit must be positive, got $limit")

  private var available: Int = limit

  private val waiters = new java.util.ArrayDeque[ExecutionSlotRequest]()

  private var timeoutScheduler: Option[ScheduledExecutorService] = None

  /** Number of slots that are currently free. */
  private[connect] def availableSlots: Int = synchronized(available)

  /** Number of requests currently waiting for a slot. */
  private[connect] def queueLength: Int = synchronized(waiters.size())

  /**
   * Request an execution slot. `onGranted` is called with the permit once a slot is granted:
   * synchronously from this method if one is available and nobody is queued, otherwise later from
   * a thread that releases a slot. It must not block.
   *
   * @param timeoutMs
   *   maximum time to wait in the queue; 0 or negative means wait indefinitely. When it elapses,
   *   the request leaves the queue and `onTimeout` is called.
   */
  def request(
      timeoutMs: Long,
      onGranted: ExecutionPermit => Unit,
      onTimeout: () => Unit): ExecutionSlotRequest = {
    val request = new ExecutionSlotRequest(this, onGranted)
    val grantNow = synchronized {
      if (waiters.isEmpty && available > 0) {
        available -= 1
        request.state = ExecutionSlotRequest.Granted
        true
      } else {
        waiters.addLast(request)
        logInfo(
          log"All execution slots are in use. Query will wait in queue. " +
            log"Queue length: ${MDC(LogKeys.THREAD_POOL_WAIT_QUEUE_SIZE, waiters.size())}")
        if (timeoutMs > 0) {
          val timeoutTask: Runnable = () => {
            if (request.abandon()) {
              logInfo(
                log"Query gave up waiting for an execution slot after " +
                  log"${MDC(LogKeys.TIMEOUT, timeoutMs)} ms.")
              onTimeout()
            }
          }
          request.timeoutTask =
            Some(scheduler.schedule(timeoutTask, timeoutMs, TimeUnit.MILLISECONDS))
        }
        false
      }
    }
    if (grantNow) {
      grant(request)
    }
    request
  }

  private[service] def abandon(request: ExecutionSlotRequest): Boolean = {
    val abandoned = synchronized {
      if (request.state == ExecutionSlotRequest.Waiting) {
        waiters.remove(request)
        request.state = ExecutionSlotRequest.Abandoned
        true
      } else {
        false
      }
    }
    if (abandoned) {
      request.timeoutTask.foreach(_.cancel(false))
    }
    abandoned
  }

  /** Return a slot: grant it to the oldest waiting request, or make it available. */
  private[connect] def release(): Unit = {
    val next = synchronized {
      val next = waiters.pollFirst()
      if (next == null) {
        available += 1
      } else {
        next.state = ExecutionSlotRequest.Granted
      }
      next
    }
    if (next != null) {
      next.timeoutTask.foreach(_.cancel(false))
      grant(next)
    }
  }

  /** Invoke the callback of a granted request, outside of the lock. */
  private def grant(request: ExecutionSlotRequest): Unit = {
    val permit = new ExecutionPermit(this)
    try {
      request.onGranted(permit)
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to start an execution that was granted an execution slot", e)
        permit.release()
    }
  }

  private def scheduler: ScheduledExecutorService = synchronized {
    timeoutScheduler.getOrElse {
      val executor =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-connect-execution-slots")
      timeoutScheduler = Some(executor)
      executor
    }
  }

  /** Stop the timeout scheduler. Waiting requests do not time out anymore. */
  private[connect] def stop(): Unit = synchronized {
    timeoutScheduler.foreach(_.shutdownNow())
    timeoutScheduler = None
  }
}
