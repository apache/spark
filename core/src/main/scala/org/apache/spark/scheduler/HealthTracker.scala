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

package org.apache.spark.scheduler

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkContext}
import org.apache.spark.internal.{config, Logging, MDC}
import org.apache.spark.internal.LogKey.HOST
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * HealthTracker is designed to track problematic executors and nodes.  It supports excluding
 * executors and nodes across an entire application (with a periodic expiry). TaskSetManagers add
 * additional logic for exclusion of executors and nodes for individual tasks and stages which
 * works in concert with the logic here.
 *
 * The tracker needs to deal with a variety of workloads, e.g.:
 *
 *  * bad user code -- this may lead to many task failures, but that should not count against
 *      individual executors
 *  * many small stages -- this may prevent a bad executor for having many failures within one
 *      stage, but still many failures over the entire application
 *  * "flaky" executors -- they don't fail every task, but are still faulty enough to merit
 *      excluding
 *  * missing shuffle files -- may trigger fetch failures on healthy executors.
 *
 * See the design doc on SPARK-8425 for a more in-depth discussion. Note SPARK-32037 renamed
 * the feature.
 *
 * THREADING: As with most helpers of TaskSchedulerImpl, this is not thread-safe.  Though it is
 * called by multiple threads, callers must already have a lock on the TaskSchedulerImpl.  The
 * one exception is [[excludedNodeList()]], which can be called without holding a lock.
 */
private[scheduler] class HealthTracker (
    private val listenerBus: LiveListenerBus,
    conf: SparkConf,
    allocationClient: Option[ExecutorAllocationClient],
    clock: Clock = new SystemClock()) extends Logging {

  def this(sc: SparkContext, allocationClient: Option[ExecutorAllocationClient]) = {
    this(sc.listenerBus, sc.conf, allocationClient)
  }

  HealthTracker.validateExcludeOnFailureConfs(conf)
  private val MAX_FAILURES_PER_EXEC = conf.get(config.MAX_FAILURES_PER_EXEC)
  private val MAX_FAILED_EXEC_PER_NODE = conf.get(config.MAX_FAILED_EXEC_PER_NODE)
  val EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS = HealthTracker.getExcludeOnFailureTimeout(conf)
  private val EXCLUDE_FETCH_FAILURE_ENABLED =
    conf.get(config.EXCLUDE_ON_FAILURE_FETCH_FAILURE_ENABLED)
  private val EXCLUDE_ON_FAILURE_DECOMMISSION_ENABLED =
    conf.get(config.EXCLUDE_ON_FAILURE_DECOMMISSION_ENABLED)

  /**
   * A map from executorId to information on task failures. Tracks the time of each task failure,
   * so that we can avoid excluding executors due to failures that are very far apart. We do not
   * actively remove from this as soon as tasks hit their timeouts, to avoid the time it would take
   * to do so. But it will not grow too large, because as soon as an executor gets too many
   * failures, we exclude the executor and remove its entry here.
   */
  private val executorIdToFailureList = new HashMap[String, ExecutorFailureList]()
  val executorIdToExcludedStatus = new HashMap[String, ExcludedExecutor]()
  val nodeIdToExcludedExpiryTime = new HashMap[String, Long]()
  /**
   * An immutable copy of the set of nodes that are currently excluded.  Kept in an
   * AtomicReference to make [[excludedNodeList()]] thread-safe.
   */
  private val _excludedNodeList = new AtomicReference[Set[String]](Set())
  /**
   * Time when the next excluded node will expire.  Used as a shortcut to
   * avoid iterating over all entries in the excludedNodeList when none will have expired.
   */
  var nextExpiryTime: Long = Long.MaxValue
  /**
   * Mapping from nodes to all of the executors that have been excluded on that node. We do *not*
   * remove from this when executors are removed from spark, so we can track when we get multiple
   * successive excluded executors on one node.  Nonetheless, it will not grow too large because
   * there cannot be many excluded executors on one node, before we stop requesting more
   * executors on that node, and we clean up the list of excluded executors once an executor has
   * been excluded for EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS.
   */
  val nodeToExcludedExecs = new HashMap[String, HashSet[String]]()

  /**
   * Include executors and nodes that have been excluded for at least
   * EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS
   */
  def applyExcludeOnFailureTimeout(): Unit = {
    val now = clock.getTimeMillis()
    // quickly check if we've got anything to expire that is excluded -- if not,
    // avoid doing any work
    if (now > nextExpiryTime) {
      // Apply the timeout to excluded nodes and executors
      val execsToInclude = executorIdToExcludedStatus.filter(_._2.expiryTime < now).keys
      if (execsToInclude.nonEmpty) {
        // Include any executors that have been excluded longer than the excludeOnFailure timeout.
        logInfo(s"Removing executors $execsToInclude from exclude list because the " +
          s"the executors have reached the timed out")
        execsToInclude.foreach { exec =>
          val status = executorIdToExcludedStatus.remove(exec).get
          val failedExecsOnNode = nodeToExcludedExecs(status.node)
          // post both to keep backwards compatibility
          listenerBus.post(SparkListenerExecutorUnblacklisted(now, exec))
          listenerBus.post(SparkListenerExecutorUnexcluded(now, exec))
          failedExecsOnNode.remove(exec)
          if (failedExecsOnNode.isEmpty) {
            nodeToExcludedExecs.remove(status.node)
          }
        }
      }
      val nodesToInclude = nodeIdToExcludedExpiryTime.filter(_._2 < now).keys
      if (nodesToInclude.nonEmpty) {
        // Include any nodes that have been excluded longer than the excludeOnFailure timeout.
        logInfo(s"Removing nodes $nodesToInclude from exclude list because the " +
          s"nodes have reached has timed out")
        nodesToInclude.foreach { node =>
          nodeIdToExcludedExpiryTime.remove(node)
          // post both to keep backwards compatibility
          listenerBus.post(SparkListenerNodeUnblacklisted(now, node))
          listenerBus.post(SparkListenerNodeUnexcluded(now, node))
        }
        _excludedNodeList.set(nodeIdToExcludedExpiryTime.keySet.toSet)
      }
      updateNextExpiryTime()
    }
  }

  private def updateNextExpiryTime(): Unit = {
    val execMinExpiry = if (executorIdToExcludedStatus.nonEmpty) {
      executorIdToExcludedStatus.map{_._2.expiryTime}.min
    } else {
      Long.MaxValue
    }
    val nodeMinExpiry = if (nodeIdToExcludedExpiryTime.nonEmpty) {
      nodeIdToExcludedExpiryTime.values.min
    } else {
      Long.MaxValue
    }
    nextExpiryTime = math.min(execMinExpiry, nodeMinExpiry)
  }

  private def killExecutor(exec: String, msg: String): Unit = {
    val fullMsg = if (EXCLUDE_ON_FAILURE_DECOMMISSION_ENABLED) {
      s"${msg} (actually decommissioning)"
    } else {
      msg
    }
    allocationClient match {
      case Some(a) =>
        logInfo(fullMsg)
        if (EXCLUDE_ON_FAILURE_DECOMMISSION_ENABLED) {
          a.decommissionExecutor(exec, ExecutorDecommissionInfo(fullMsg),
            adjustTargetNumExecutors = false)
        } else {
          a.killExecutors(Seq(exec), adjustTargetNumExecutors = false, countFailures = false,
            force = true)
        }
      case None =>
        logInfo(s"Not attempting to kill excluded executor id $exec " +
          s"since allocation client is not defined.")
    }
  }

  private def killExcludedExecutor(exec: String): Unit = {
    if (conf.get(config.EXCLUDE_ON_FAILURE_KILL_ENABLED)) {
      killExecutor(exec, s"Killing excluded executor id $exec since " +
        s"${config.EXCLUDE_ON_FAILURE_KILL_ENABLED.key} is set.")
    }
  }

  private[scheduler] def killExcludedIdleExecutor(exec: String): Unit = {
    killExecutor(exec,
      s"Killing excluded idle executor id $exec because of task unschedulability and trying " +
        "to acquire a new executor.")
  }

  private def killExecutorsOnExcludedNode(node: String): Unit = {
    if (conf.get(config.EXCLUDE_ON_FAILURE_KILL_ENABLED)) {
      allocationClient match {
        case Some(a) =>
          if (EXCLUDE_ON_FAILURE_DECOMMISSION_ENABLED) {
            logInfo(s"Decommissioning all executors on excluded host $node " +
              s"since ${config.EXCLUDE_ON_FAILURE_KILL_ENABLED.key} is set.")
            if (!a.decommissionExecutorsOnHost(node)) {
              logError(log"Decommissioning executors on ${MDC(HOST, node)} failed.")
            }
          } else {
            logInfo(s"Killing all executors on excluded host $node " +
              s"since ${config.EXCLUDE_ON_FAILURE_KILL_ENABLED.key} is set.")
            if (!a.killExecutorsOnHost(node)) {
              logError(log"Killing executors on node ${MDC(HOST, node)} failed.")
            }
          }
        case None =>
          logWarning(s"Not attempting to kill executors on excluded host $node " +
            s"since allocation client is not defined.")
      }
    }
  }

  def updateExcludedForFetchFailure(host: String, exec: String): Unit = {
    if (EXCLUDE_FETCH_FAILURE_ENABLED) {
      // If we exclude on fetch failures, we are implicitly saying that we believe the failure is
      // non-transient, and can't be recovered from (even if this is the first fetch failure,
      // stage is retried after just one failure, so we don't always get a chance to collect
      // multiple fetch failures).
      // If the external shuffle-service is on, then every other executor on this node would
      // be suffering from the same issue, so we should exclude (and potentially kill) all
      // of them immediately.

      val now = clock.getTimeMillis()
      val expiryTimeForNewExcludes = now + EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS

      if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
        if (!nodeIdToExcludedExpiryTime.contains(host)) {
          logInfo(s"excluding node $host due to fetch failure of external shuffle service")

          nodeIdToExcludedExpiryTime.put(host, expiryTimeForNewExcludes)
          // post both to keep backwards compatibility
          listenerBus.post(SparkListenerNodeBlacklisted(now, host, 1))
          listenerBus.post(SparkListenerNodeExcluded(now, host, 1))
          _excludedNodeList.set(nodeIdToExcludedExpiryTime.keySet.toSet)
          killExecutorsOnExcludedNode(host)
          updateNextExpiryTime()
        }
      } else if (!executorIdToExcludedStatus.contains(exec)) {
        logInfo(s"Excluding executor $exec due to fetch failure")

        executorIdToExcludedStatus.put(exec, ExcludedExecutor(host, expiryTimeForNewExcludes))
        // We hardcoded number of failure tasks to 1 for fetch failure, because there's no
        // reattempt for such failure.
        // post both to keep backwards compatibility
        listenerBus.post(SparkListenerExecutorBlacklisted(now, exec, 1))
        listenerBus.post(SparkListenerExecutorExcluded(now, exec, 1))
        updateNextExpiryTime()
        killExcludedExecutor(exec)

        val excludedExecsOnNode = nodeToExcludedExecs.getOrElseUpdate(host, HashSet[String]())
        excludedExecsOnNode += exec
      }
    }
  }

  def updateExcludedForSuccessfulTaskSet(
      stageId: Int,
      stageAttemptId: Int,
      failuresByExec: HashMap[String, ExecutorFailuresInTaskSet]): Unit = {
    // if any tasks failed, we count them towards the overall failure count for the executor at
    // this point.
    val now = clock.getTimeMillis()
    failuresByExec.foreach { case (exec, failuresInTaskSet) =>
      val appFailuresOnExecutor =
        executorIdToFailureList.getOrElseUpdate(exec, new ExecutorFailureList)
      appFailuresOnExecutor.addFailures(stageId, stageAttemptId, failuresInTaskSet)
      appFailuresOnExecutor.dropFailuresWithTimeoutBefore(now)
      val newTotal = appFailuresOnExecutor.numUniqueTaskFailures

      val expiryTimeForNewExcludes = now + EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS
      // If this pushes the total number of failures over the threshold, exclude the executor.
      // If its already excluded, we avoid "re-excluding" (which can happen if there were
      // other tasks already running in another taskset when it got excluded), because it makes
      // some of the logic around expiry times a little more confusing.  But it also wouldn't be a
      // problem to re-exclude, with a later expiry time.
      if (newTotal >= MAX_FAILURES_PER_EXEC && !executorIdToExcludedStatus.contains(exec)) {
        logInfo(s"Excluding executor id: $exec because it has $newTotal" +
          s" task failures in successful task sets")
        val node = failuresInTaskSet.node
        executorIdToExcludedStatus.put(exec, ExcludedExecutor(node, expiryTimeForNewExcludes))
        // post both to keep backwards compatibility
        listenerBus.post(SparkListenerExecutorBlacklisted(now, exec, newTotal))
        listenerBus.post(SparkListenerExecutorExcluded(now, exec, newTotal))
        executorIdToFailureList.remove(exec)
        updateNextExpiryTime()
        killExcludedExecutor(exec)

        // In addition to excluding the executor, we also update the data for failures on the
        // node, and potentially exclude the entire node as well.
        val excludedExecsOnNode = nodeToExcludedExecs.getOrElseUpdate(node, HashSet[String]())
        excludedExecsOnNode += exec
        // If the node is already excluded, we avoid adding it again with a later expiry
        // time.
        if (excludedExecsOnNode.size >= MAX_FAILED_EXEC_PER_NODE &&
            !nodeIdToExcludedExpiryTime.contains(node)) {
          logInfo(s"Excluding node $node because it has ${excludedExecsOnNode.size} " +
            s"executors excluded: ${excludedExecsOnNode}")
          nodeIdToExcludedExpiryTime.put(node, expiryTimeForNewExcludes)
          // post both to keep backwards compatibility
          listenerBus.post(SparkListenerNodeBlacklisted(now, node, excludedExecsOnNode.size))
          listenerBus.post(SparkListenerNodeExcluded(now, node, excludedExecsOnNode.size))
          _excludedNodeList.set(nodeIdToExcludedExpiryTime.keySet.toSet)
          killExecutorsOnExcludedNode(node)
        }
      }
    }
  }

  def isExecutorExcluded(executorId: String): Boolean = {
    executorIdToExcludedStatus.contains(executorId)
  }

  /**
   * Get the full set of nodes that are excluded.  Unlike other methods in this class, this *IS*
   * thread-safe -- no lock required on a taskScheduler.
   */
  def excludedNodeList(): Set[String] = {
    _excludedNodeList.get()
  }

  def isNodeExcluded(node: String): Boolean = {
    nodeIdToExcludedExpiryTime.contains(node)
  }

  def handleRemovedExecutor(executorId: String): Unit = {
    // We intentionally do not clean up executors that are already excluded in
    // nodeToExcludedExecs, so that if another executor on the same node gets excluded, we can
    // exclude the entire node. We also can't clean up executorIdToExcludedStatus, so we can
    // eventually remove the executor after the timeout. Despite not clearing those structures
    // here, we don't expect they will grow too big since you won't get too many executors on one
    // node, and the timeout will clear it up periodically in any case.
    executorIdToFailureList -= executorId
  }

  /**
   * Tracks all failures for one executor (that have not passed the timeout).
   *
   * In general we actually expect this to be extremely small, since it won't contain more than the
   * maximum number of task failures before an executor is failed (default 2).
   */
  private[scheduler] final class ExecutorFailureList extends Logging {

    private case class TaskId(stage: Int, stageAttempt: Int, taskIndex: Int)

    /**
     * All failures on this executor in successful task sets.
     */
    private var failuresAndExpiryTimes = ArrayBuffer[(TaskId, Long)]()
    /**
     * As an optimization, we track the min expiry time over all entries in failuresAndExpiryTimes
     * so its quick to tell if there are any failures with expiry before the current time.
     */
    private var minExpiryTime = Long.MaxValue

    def addFailures(
        stage: Int,
        stageAttempt: Int,
        failuresInTaskSet: ExecutorFailuresInTaskSet): Unit = {
      failuresInTaskSet.taskToFailureCountAndFailureTime.foreach {
        case (taskIdx, (_, failureTime)) =>
          val expiryTime = failureTime + EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS
          failuresAndExpiryTimes += ((TaskId(stage, stageAttempt, taskIdx), expiryTime))
          if (expiryTime < minExpiryTime) {
            minExpiryTime = expiryTime
          }
      }
    }

    /**
     * The number of unique tasks that failed on this executor.  Only counts failures within the
     * timeout, and in successful tasksets.
     */
    def numUniqueTaskFailures: Int = failuresAndExpiryTimes.size

    def isEmpty: Boolean = failuresAndExpiryTimes.isEmpty

    /**
     * Apply the timeout to individual tasks.  This is to prevent one-off failures that are very
     * spread out in time (and likely have nothing to do with problems on the executor) from
     * triggering exclusion.  However, note that we do *not* remove executors and nodes from
     * being excluded as we expire individual task failures -- each have their own timeout.  E.g.,
     * suppose:
     *  * timeout = 10, maxFailuresPerExec = 2
     *  * Task 1 fails on exec 1 at time 0
     *  * Task 2 fails on exec 1 at time 5
     * -->  exec 1 is excluded from time 5 - 15.
     * This is to simplify the implementation, as well as keep the behavior easier to understand
     * for the end user.
     */
    def dropFailuresWithTimeoutBefore(dropBefore: Long): Unit = {
      if (minExpiryTime < dropBefore) {
        var newMinExpiry = Long.MaxValue
        val newFailures = new ArrayBuffer[(TaskId, Long)]
        failuresAndExpiryTimes.foreach { case (task, expiryTime) =>
          if (expiryTime >= dropBefore) {
            newFailures += ((task, expiryTime))
            if (expiryTime < newMinExpiry) {
              newMinExpiry = expiryTime
            }
          }
        }
        failuresAndExpiryTimes = newFailures
        minExpiryTime = newMinExpiry
      }
    }

    override def toString(): String = {
      s"failures = $failuresAndExpiryTimes"
    }
  }

}

private[spark] object HealthTracker extends Logging {

  private val DEFAULT_TIMEOUT = "1h"

  /**
   * Returns true if the excludeOnFailure is enabled, based on checking the configuration
   * in the following order:
   * 1. Is it specifically enabled or disabled?
   * 2. Is it enabled via the legacy timeout conf?
   * 3. Default is off
   */
  def isExcludeOnFailureEnabled(conf: SparkConf): Boolean = {
    conf.get(config.EXCLUDE_ON_FAILURE_ENABLED) match {
      case Some(enabled) =>
        enabled
      case None =>
        // if they've got a non-zero setting for the legacy conf, always enable it,
        // otherwise, use the default.
        val legacyKey = config.EXCLUDE_ON_FAILURE_LEGACY_TIMEOUT_CONF.key
        conf.get(config.EXCLUDE_ON_FAILURE_LEGACY_TIMEOUT_CONF).exists { legacyTimeout =>
          if (legacyTimeout == 0) {
            logWarning(s"Turning off excludeOnFailure due to legacy configuration: $legacyKey == 0")
            false
          } else {
            logWarning(s"Turning on excludeOnFailure due to legacy configuration: $legacyKey > 0")
            true
          }
        }
    }
  }

  def getExcludeOnFailureTimeout(conf: SparkConf): Long = {
    conf.get(config.EXCLUDE_ON_FAILURE_TIMEOUT_CONF).getOrElse {
      conf.get(config.EXCLUDE_ON_FAILURE_LEGACY_TIMEOUT_CONF).getOrElse {
        Utils.timeStringAsMs(DEFAULT_TIMEOUT)
      }
    }
  }

  /**
   * Verify that exclude on failure configurations are consistent; if not, throw an exception.
   * Should only be called if excludeOnFailure is enabled.
   *
   * The configuration is expected to adhere to a few invariants.  Default values
   * follow these rules of course, but users may unwittingly change one configuration
   * without making the corresponding adjustment elsewhere. This ensures we fail-fast when
   * there are such misconfigurations.
   */
  def validateExcludeOnFailureConfs(conf: SparkConf): Unit = {

    def mustBePos(k: String, v: String): Unit = {
      throw new IllegalArgumentException(s"$k was $v, but must be > 0.")
    }

    Seq(
      config.MAX_TASK_ATTEMPTS_PER_EXECUTOR,
      config.MAX_TASK_ATTEMPTS_PER_NODE,
      config.MAX_FAILURES_PER_EXEC_STAGE,
      config.MAX_FAILED_EXEC_PER_NODE_STAGE,
      config.MAX_FAILURES_PER_EXEC,
      config.MAX_FAILED_EXEC_PER_NODE
    ).foreach { config =>
      val v = conf.get(config)
      if (v <= 0) {
        mustBePos(config.key, v.toString)
      }
    }

    val timeout = getExcludeOnFailureTimeout(conf)
    if (timeout <= 0) {
      // first, figure out where the timeout came from, to include the right conf in the message.
      conf.get(config.EXCLUDE_ON_FAILURE_TIMEOUT_CONF) match {
        case Some(t) =>
          mustBePos(config.EXCLUDE_ON_FAILURE_TIMEOUT_CONF.key, timeout.toString)
        case None =>
          mustBePos(config.EXCLUDE_ON_FAILURE_LEGACY_TIMEOUT_CONF.key, timeout.toString)
      }
    }

    val maxTaskFailures = conf.get(config.TASK_MAX_FAILURES)
    val maxNodeAttempts = conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)

    if (maxNodeAttempts >= maxTaskFailures) {
      throw new IllegalArgumentException(s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key} " +
        s"( = ${maxNodeAttempts}) was >= ${config.TASK_MAX_FAILURES.key} " +
        s"( = ${maxTaskFailures} ). Though excludeOnFailure is enabled, with this configuration, " +
        s"Spark will not be robust to one bad node. Decrease " +
        s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key}, increase ${config.TASK_MAX_FAILURES.key}, " +
        s"or disable excludeOnFailure with ${config.EXCLUDE_ON_FAILURE_ENABLED.key}")
    }
  }
}

private final case class ExcludedExecutor(node: String, expiryTime: Long)
