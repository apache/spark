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

import com.codahale.metrics.MetricRegistry

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.Source
import org.apache.spark.util.Clock

private[spark] object ExecutorPodsBackoffController {
  sealed trait State

  case object NormalState extends State

  case class BackoffState(
    requestedExecutors: mutable.HashSet[Long] = mutable.HashSet.empty,
    // time reference for backoff delay calculation - set when entering Backoff
    // and updated after each pod request
    var delayReferenceTime: Long) extends State
}

/**
 * Applies exponential backoff delays for executor pod requests when pods repeatedly fail to start,
 * reducing load on the Kubernetes infrastructure (control plane, Istio, etc).
 *
 * Operates as a state machine with two states:
 *  - Normal: No extra delay is introduced.
 *    Transitions to Backoff when startup failures count in a sliding time window exceeds threshold.
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

  private val failureThreshold =
    conf.get(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_FAILURE_THRESHOLD)
  private val failureIntervalMs =
    conf.get(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_FAILURE_INTERVAL)
  private val initialBackoffDelayMs =
    conf.get(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_INITIAL_DELAY)
  private val maxBackoffDelayMs = conf.get(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_MAX_DELAY)

  private var currentState: State = NormalState

  // executors that were requested to launch but haven't been confirmed as started yet
  private val executorsAwaitingStartedConfirmation = mutable.HashSet.empty[Long]

  // startup failures within the sliding time window (timestamp, executorId)
  private val startupFailuresInInterval: mutable.Queue[(Long, Long)] = mutable.Queue.empty

  val metricsSource = new ExecutorPodsBackoffControllerSource()

  /**
   * Record that a pod request is being made.
   */
  def recordPodRequest(executorId: Long): Unit = synchronized {
    logDebug(s"Recording pod request for executor $executorId")
    executorsAwaitingStartedConfirmation.add(executorId)
    currentState match {
      case backoff: BackoffState =>
        backoff.requestedExecutors.add(executorId)
        backoff.delayReferenceTime = clock.getTimeMillis()
      case _ =>
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
      metricsSource.startupFailureCounter.inc()

      // add failure to the sliding time window and remove expired ones
      startupFailuresInInterval.enqueue((currentTime, executorId))
      evictExpiredFailures(startupFailuresInInterval)

      currentState match {
        case NormalState =>
          if (startupFailuresInInterval.size >= failureThreshold) {
            logWarning(s"Executor startup failures reached ${startupFailuresInInterval.size} " +
              s"in ${failureIntervalMs / 1000}s. Transitioning to Backoff state. " +
              s"Recent failed executors IDs: ${startupFailuresInInterval.map(_._2).mkString(", ")}")
            currentState = BackoffState(delayReferenceTime = currentTime)
            metricsSource.backoffEntryCounter.inc()
          }

        case backoff: BackoffState =>
          if (backoff.requestedExecutors.contains(executorId)) {
            logWarning(s"Executor allocation failed in Backoff state (executor ID: $executorId, " +
              s"number of requests made in backoff state: ${backoff.requestedExecutors.size})")
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
    currentState match {
      case backoff: BackoffState if backoff.requestedExecutors.contains(executorId) =>
        logInfo(s"Executor $executorId successfully started in Backoff. " +
          s"Transitioning to Normal state.")
        currentState = NormalState
        metricsSource.backoffExitCounter.inc()
      case _ =>
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
      case NormalState => true
      case backoff: BackoffState =>
        val currentTime = clock.getTimeMillis()
        val requiredDelay = calculateBackoffDelay(backoff)
        currentTime >= backoff.delayReferenceTime + requiredDelay
    }
  }

  def isBackoffState(): Boolean = synchronized {
    currentState match {
      case _: BackoffState => true
      case _ => false
    }
  }

  def isNormalState(): Boolean = synchronized {
    currentState match {
      case NormalState => true
      case _ => false
    }
  }

  def currentStateDescription(): String = synchronized {
    currentState match {
      case NormalState =>
        s"Normal (recent startup failures: " +
          s"${evictExpiredFailures(startupFailuresInInterval)})"
      case backoff: BackoffState =>
        s"Backoff (attempts in backoff: ${backoff.requestedExecutors.size}, " +
          s"current delay: ${calculateBackoffDelay(backoff) / 1000}s)"
    }
  }

  private[k8s] def calculateBackoffDelay(backoff: BackoffState): Long = {
    // use BigInt to avoid overflow
    val delay = BigInt(initialBackoffDelayMs) * BigInt(2).pow(backoff.requestedExecutors.size)
    delay.min(maxBackoffDelayMs).toLong
  }

  /**
   * evicts failures outside the sliding window and returns the count of remaining failures
   */
  private def evictExpiredFailures(failuresInInterval: mutable.Queue[(Long, Long)]): Int = {
    val currentTime = clock.getTimeMillis()
    while (failuresInInterval.nonEmpty &&
      currentTime - failuresInInterval.head._1 > failureIntervalMs) {
      failuresInInterval.dequeue()
    }
    failuresInInterval.size
  }

  // for tests only
  def startupFailureCountInWindow(): Int = synchronized {
    evictExpiredFailures(startupFailuresInInterval)
  }
}

private[spark] class ExecutorPodsBackoffControllerSource extends Source {

  override val sourceName: String = "ExecutorPodsBackoffController"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  val startupFailureCounter = metricRegistry.counter(MetricRegistry.name("startupFailureCount"))
  val backoffEntryCounter = metricRegistry.counter(MetricRegistry.name("backoffEntryCount"))
  val backoffExitCounter = metricRegistry.counter(MetricRegistry.name("backoffExitCount"))
}
