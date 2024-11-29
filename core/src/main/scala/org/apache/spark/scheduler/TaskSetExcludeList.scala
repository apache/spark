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

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging, LogKeys, MDC}
import org.apache.spark.util.Clock

/**
 * Handles excluding executors and nodes within a taskset.  This includes excluding specific
 * (task, executor) / (task, nodes) pairs, and also completely excluding executors and nodes
 * for the entire taskset.
 *
 * It also must store sufficient information in task failures for application level exclusion,
 * which is handled by [[HealthTracker]].  Note that HealthTracker does not know anything
 * about task failures until a taskset completes successfully.
 *
 * If isDryRun is true, then this class will only function to store information for application
 * level exclusion, and will not actually exclude any tasks in task/stage level.
 *
 * THREADING:  This class is a helper to [[TaskSetManager]]; as with the methods in
 * [[TaskSetManager]] this class is designed only to be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 */
private[scheduler] class TaskSetExcludelist(
    private val listenerBus: LiveListenerBus,
    val conf: SparkConf,
    val stageId: Int,
    val stageAttemptId: Int,
    val clock: Clock,
    val isDryRun: Boolean = false) extends Logging {

  private val MAX_TASK_ATTEMPTS_PER_EXECUTOR = conf.get(config.MAX_TASK_ATTEMPTS_PER_EXECUTOR)
  private val MAX_TASK_ATTEMPTS_PER_NODE = conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)
  private val MAX_FAILURES_PER_EXEC_STAGE = conf.get(config.MAX_FAILURES_PER_EXEC_STAGE)
  private val MAX_FAILED_EXEC_PER_NODE_STAGE = conf.get(config.MAX_FAILED_EXEC_PER_NODE_STAGE)

  /**
   * A map from each executor to the task failures on that executor.  This is used for exclusion
   * within this taskset, and it is also relayed onto [[HealthTracker]] for app-level
   * exlucsion if this taskset completes successfully.
   */
  val execToFailures = new HashMap[String, ExecutorFailuresInTaskSet]()

  /**
   * Map from node to all executors on it with failures.  Needed because we want to know about
   * executors on a node even after they have died. (We don't want to bother tracking the
   * node -> execs mapping in the usual case when there aren't any failures).
   */
  private val nodeToExecsWithFailures = new HashMap[String, HashSet[String]]()
  private val nodeToExcludedTaskIndexes = new HashMap[String, HashSet[Int]]()
  private val excludedExecs = new HashSet[String]()
  private val excludedNodes = new HashSet[String]()

  private var latestFailureReason: String = null

  /**
   * Get the most recent failure reason of this TaskSet.
   */
  def getLatestFailureReason: String = {
    latestFailureReason
  }

  /**
   * Return true if this executor is excluded for the given task.  This does *not*
   * need to return true if the executor is excluded for the entire stage, or excluded
   * for the entire application.  That is to keep this method as fast as possible in the inner-loop
   * of the scheduler, where those filters will have already been applied.
   */
  def isExecutorExcludedForTask(executorId: String, index: Int): Boolean = {
    !isDryRun && execToFailures.get(executorId).exists { execFailures =>
      execFailures.getNumTaskFailures(index) >= MAX_TASK_ATTEMPTS_PER_EXECUTOR
    }
  }

  def isNodeExcludedForTask(node: String, index: Int): Boolean = {
    !isDryRun && nodeToExcludedTaskIndexes.get(node).exists(_.contains(index))
  }

  /**
   * Return true if this executor is excluded for the given stage.  Completely ignores whether
   * the executor is excluded for the entire application (or anything to do with the node the
   * executor is on).  That is to keep this method as fast as possible in the inner-loop of the
   * scheduler, where those filters will already have been applied.
   */
  def isExecutorExcludedForTaskSet(executorId: String): Boolean = {
    !isDryRun && excludedExecs.contains(executorId)
  }

  def isNodeExcludedForTaskSet(node: String): Boolean = {
    !isDryRun && excludedNodes.contains(node)
  }

  private[scheduler] def updateExcludedForFailedTask(
      host: String,
      exec: String,
      index: Int,
      failureReason: String): Unit = {
    latestFailureReason = failureReason
    val execFailures = execToFailures.getOrElseUpdate(exec, new ExecutorFailuresInTaskSet(host))
    execFailures.updateWithFailure(index, clock.getTimeMillis())

    // check if this task has also failed on other executors on the same host -- if its gone
    // over the limit, exclude this task from the entire host.
    val execsWithFailuresOnNode = nodeToExecsWithFailures.getOrElseUpdate(host, new HashSet())
    execsWithFailuresOnNode += exec
    val failuresOnHost = execsWithFailuresOnNode.iterator.flatMap { exec =>
      execToFailures.get(exec).map { failures =>
        // We count task attempts here, not the number of unique executors with failures.  This is
        // because jobs are aborted based on the number task attempts; if we counted unique
        // executors, it would be hard to config to ensure that you try another
        // node before hitting the max number of task failures.
        failures.getNumTaskFailures(index)
      }
    }.sum
    if (failuresOnHost >= MAX_TASK_ATTEMPTS_PER_NODE) {
      nodeToExcludedTaskIndexes.getOrElseUpdate(host, new HashSet()) += index
    }

    // Check if enough tasks have failed on the executor to exclude it for the entire stage.
    val numFailures = execFailures.numUniqueTasksWithFailures
    if (numFailures >= MAX_FAILURES_PER_EXEC_STAGE) {
      if (excludedExecs.add(exec)) {
        logInfo(log"Excluding executor ${MDC(LogKeys.EXECUTOR_ID, exec)} for stage " +
          log"${MDC(LogKeys.STAGE_ID, stageId)}")
        // This executor has been excluded for this stage.  Let's check if it
        // the whole node should be excluded.
        val excludedExecutorsOnNode =
          execsWithFailuresOnNode.intersect(excludedExecs)
        val now = clock.getTimeMillis()
        // SparkListenerExecutorBlacklistedForStage is deprecated but post both events
        // to keep backward compatibility
        listenerBus.post(
          SparkListenerExecutorBlacklistedForStage(now, exec, numFailures, stageId, stageAttemptId))
        listenerBus.post(
          SparkListenerExecutorExcludedForStage(now, exec, numFailures, stageId, stageAttemptId))
        val numFailExec = excludedExecutorsOnNode.size
        if (numFailExec >= MAX_FAILED_EXEC_PER_NODE_STAGE) {
          if (excludedNodes.add(host)) {
            logInfo(log"Excluding ${MDC(LogKeys.HOST, host)} for " +
              log"stage ${MDC(LogKeys.STAGE_ID, stageId)}")
            // SparkListenerNodeBlacklistedForStage is deprecated but post both events
            // to keep backward compatibility
            listenerBus.post(
              SparkListenerNodeBlacklistedForStage(now, host, numFailExec, stageId, stageAttemptId))
            listenerBus.post(
              SparkListenerNodeExcludedForStage(now, host, numFailExec, stageId, stageAttemptId))
          }
        }
      }
    }
  }
}

private[scheduler] object TaskSetExcludelist {

  /**
   * Returns true if the excludeOnFailure is enabled on the task/stage level,
   * based on checking the configuration in the following order:
   * 1. Is taskset level exclusion specifically enabled or disabled?
   * 2. Is overall exclusion feature enabled or disabled?
   * 3. Default is off
   */
  def isExcludeOnFailureEnabled(conf: SparkConf): Boolean = {
    conf.get(config.EXCLUDE_ON_FAILURE_ENABLED_TASK_AND_STAGE)
      .orElse(conf.get(config.EXCLUDE_ON_FAILURE_ENABLED)).getOrElse(false)
  }
}
