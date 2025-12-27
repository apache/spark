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
package org.apache.spark.scheduler.cluster.k8s

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.util.Clock

/**
 * Applies exponential backoff delays for executor pod requests when pods repeatedly fail to start,
 * reducing load on the Kubernetes infrastructure (control plane, Istio, etc).
 *
 * Operates as a state machine with two states:
 *  - Normal: No extra delay is introduced. Tracks startup failures in a sliding time window.
 *    Transitions to Backoff when failure count exceeds threshold.
 *  - Backoff: Applies exponential backoff delays between requests.
 *    Transitions back to Normal when an executor requested during Backoff is seen started.
 *
 * Only startup failures (executors that never started) are counted for backoff.
 *
 * Thread-safe: all public methods are synchronized as they can be called from multiple threads
 * (ExecutorPodsAllocator and KubernetesClusterSchedulerBackend)
 */
private[spark] class ExecutorPodsBackoffController(
    conf: SparkConf,
    clock: Clock) extends Logging {

  import ExecutorPodsBackoffController._

  // configs
  private val failureThreshold =
    conf.get(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_FAILURE_THRESHOLD)
  private val failureIntervalMs =
    conf.get(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_FAILURE_INTERVAL)
  private val initialBackoffDelayMs =
    conf.get(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_INITIAL_DELAY)
  private val maxBackoffDelayMs = conf.get(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_MAX_DELAY)

  // current state
  private var currentState: BackoffState = Normal

  // startup failure within the sliding time window
  private val startupFailuresInInterval = mutable.Queue[(Long, Long)]() // (timestamp, executorId)

  // executors that were requested to launch but haven't been confirmed as started yet
  private val executorsAwaitingStartedConfirmation = mutable.HashSet.empty[Long]

  // executors that were requested in Backoff state. Executor is removed from the set on failure.
  private val executorsRequestedInBackoff = mutable.HashSet.empty[Long]
  private var executorRequestsCountInBackoff = 0
  // time reference for backoff delay calculation - set when entering backoff
  // and updated after each allocation attempt in backoff
  private var delayReferenceTime = 0L

  /**
   * Record that a pod request is being made.
   */
  def recordPodRequest(executorId: Long): Unit = synchronized {
    logDebug(s"Recording pod request for executor $executorId in state $currentState")

    executorsAwaitingStartedConfirmation.add(executorId)

    if (currentState == Backoff) {
      executorRequestsCountInBackoff = executorRequestsCountInBackoff + 1
      executorsRequestedInBackoff.add(executorId)
      // update reference time so next request must wait the delay
      delayReferenceTime = clock.getTimeMillis()
    }
  }

  /**
   * Records a failure - either pod request failure OR pod failure.
   *
   * For backoff we count only executors that never started.
   * Can be called multiple times for the same executor - duplicates are ignored.
   */
  def recordFailure(executorId: Long): Unit = synchronized {
    val currentTime = clock.getTimeMillis()

    // only track startup failures (executors that never started)
    if (!executorsAwaitingStartedConfirmation.contains(executorId)) {
      logDebug(s"Ignoring failure for executor $executorId - not awaiting startup confirmation.")
    } else {
      logDebug(s"Recording startup failure for executor $executorId")
      // remove from set since we're processing its failure
      // ensures we don't count failures for the same executor several times
      executorsAwaitingStartedConfirmation.remove(executorId)

      // add failure to the sliding time window and remove expired ones
      startupFailuresInInterval.enqueue((currentTime, executorId))
      evictExpiredFailures()

      currentState match {
        case Normal =>
          if (startupFailuresInInterval.size >= failureThreshold) {
            logWarning(log"Executor startup failure threshold reached " +
              log"(${MDC(LogKeys.NUM_FAILURES, startupFailuresInInterval.size)} failures in " +
              log"${MDC(LogKeys.INTERVAL, failureIntervalMs / 1000)}s). " +
              log"Transitioning to Backoff state. Recent failed executor IDs: " +
              log"${MDC(LogKeys.EXECUTOR_IDS, startupFailuresInInterval.map(_._2).mkString(", "))}")
            // transition to Backoff
            currentState = Backoff
            delayReferenceTime = currentTime
          }

        case Backoff =>
          if (executorsRequestedInBackoff.contains(executorId)) {
            // remove from set so it's not ever-growing
            executorsRequestedInBackoff.remove(executorId)
            logWarning(log"Executor allocation failed in Backoff state " +
              log"(executor ID: ${MDC(LogKeys.EXECUTOR_ID, executorId)}, " +
              log"number of requests made in backoff state: " +
              log"${MDC(LogKeys.NUM_REQUESTS, executorRequestsCountInBackoff)})")
          } else {
            logDebug(s"Failed executor $executorId was requested before Backoff state.")
          }
      }
    }
  }

  /**
   * Records a successful executor startup.
   * If in Backoff state and this executor was requested during backoff, transitions to Normal.
   * Can be called multiple times for the same executor - duplicates are ignored.
   */
  def recordExecutorStarted(executorId: Long): Unit = synchronized {
    logDebug(s"Recording executor $executorId as started.")
    executorsAwaitingStartedConfirmation.remove(executorId)

    // only transition to Normal if this executor was requested during Backoff state.
    // Success of executors requested before Backoff doesn't prove the issue is resolved.
    if (currentState == Backoff && executorsRequestedInBackoff.contains(executorId)) {
      logInfo(log"Executor ${MDC(LogKeys.EXECUTOR_ID, executorId)} successfully started in " +
        log"Backoff. Transitioning to Normal state.")
      currentState = Normal
      startupFailuresInInterval.clear()
      executorsAwaitingStartedConfirmation.clear()
      executorsRequestedInBackoff.clear()
      executorRequestsCountInBackoff = 0
      delayReferenceTime = 0
    }
  }

  /**
   * Record that executor was deleted.
   */
  def recordDeleted(executorId: Long): Unit = synchronized {
    logDebug(s"Recording deleted executor $executorId")
    // ensure the set is not ever-growing with deleted executors
    // that were not reported as failed or started
    executorsAwaitingStartedConfirmation.remove(executorId)
  }

  /**
   * Checks if executor request is allowed.
   * Always allowed in Normal state.
   * In Backoff state, checks if the backoff delay has passed.
   */
  def canRequestNow(): Boolean = synchronized {
    currentState match {
      case Normal => true
      case Backoff =>
        val currentTime = clock.getTimeMillis()
        val requiredDelay = calculateBackoffDelay()
        currentTime >= delayReferenceTime + requiredDelay
    }
  }

  def getCurrentState(): BackoffState = synchronized {
    currentState
  }

  def isInBackoffState(): Boolean = synchronized {
    currentState == Backoff
  }

  def isInNormalState(): Boolean = synchronized {
    currentState == Normal
  }

  def startupFailureCountInWindow(): Int = synchronized {
    evictExpiredFailures()
  }

  /**
   * A human-readable string describing the current state.
   */
  def currentStateDescription(): String = synchronized {
    currentState match {
      case Normal =>
        s"Normal (recent startup failures: ${evictExpiredFailures()})"
      case Backoff =>
        s"Backoff (attempts in backoff: $executorRequestsCountInBackoff, " +
          s"current delay: ${calculateBackoffDelay() / 1000}s)"
    }
  }

  def calculateBackoffDelay(): Long = synchronized {
    // use BigInt to avoid overflow
    val delay = BigInt(initialBackoffDelayMs) * BigInt(2).pow(executorRequestsCountInBackoff)
    delay.min(maxBackoffDelayMs).toLong
  }

  /**
   * evicts failures outside the sliding window and returns the count of remaining failures
   */
  private def evictExpiredFailures(): Int = {
    val currentTime = clock.getTimeMillis()
    while (startupFailuresInInterval.nonEmpty &&
      currentTime - startupFailuresInInterval.head._1 > failureIntervalMs) {
      startupFailuresInInterval.dequeue()
    }
    startupFailuresInInterval.size
  }
}

private[spark] object ExecutorPodsBackoffController {
  sealed trait BackoffState
  case object Normal extends BackoffState
  case object Backoff extends BackoffState
}
