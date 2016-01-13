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

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.util.Clock

/**
 * The interface to determine executor blacklist and node blacklist.
 */
private [scheduler] trait BlacklistStrategy {
  /** Define a time interval to expire failure information of executors */
  val expireTimeInMilliseconds: Long

  /** Return executors in blacklist which are related to given stage and partition */
  def getExecutorBlacklist(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus],
      atomTask: StageAndPartition,
      clock: Clock): Set[String]

  /** Return all nodes in blacklist */
  def getNodeBlacklist(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus],
      clock: Clock): Set[String]

  /** Return all nodes in blacklist for specified stage. By default it returns the same result as
   *  getNodeBlacklist. It could be override in strategy implementation.
   */
  def getNodeBlacklistForStage(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus],
      stageId: Int,
      clock: Clock): Set[String] = getNodeBlacklist(executorIdToFailureStatus, clock)

  /**
   * Choose which executors should be removed from blacklist. Return true if any executors are
   * removed from the blacklist, false otherwise. The default implementation removes executors from
   * the blacklist after [[expireTimeInMilliseconds]]
   */
  def expireExecutorsInBlackList(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus],
      clock: Clock): Boolean = {
    val now = clock.getTimeMillis()
    val expiredKey = executorIdToFailureStatus.filter {
      case (executorid, failureStatus) => {
        (now - failureStatus.updatedTime) >= expireTimeInMilliseconds
      }
    }.keySet

    if (expiredKey.isEmpty) {
      false
    } else {
      executorIdToFailureStatus --= expiredKey
      true
    }
  }
}

/**
 * This strategy adds an executor to the blacklist for all tasks when the executor has too many
 * task failures. An executor is placed in the blacklist when there are more than
 * [[maxFailedTasks]] failed tasks. Furthermore, all executors in one node are put into the
 * blacklist if there are more than [[maxBlacklistedExecutors]] blacklisted executors on one node.
 * The benefit of this strategy is that different taskSets can learn experience from other taskSet
 * to avoid allocating tasks on problematic executors.
 */
private[scheduler] class ExecutorAndNodeStrategy(
    maxFailedTasks: Int,
    maxBlacklistedExecutors: Int,
    val expireTimeInMilliseconds: Long
  ) extends BlacklistStrategy {

  private def getExecutorBlacklistInfo(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus]) = {
    executorIdToFailureStatus.filter{
      case (id, failureStatus) => failureStatus.totalNumFailures > maxFailedTasks
    }
  }

  // As this is a task unrelated strategy, the input StageAndPartition info will be ignored
  def getExecutorBlacklist(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus],
      atomTask: StageAndPartition,
      clock: Clock): Set[String] = {
    getExecutorBlacklistInfo(executorIdToFailureStatus).keys.toSet
  }

  def getNodeBlacklist(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus],
      clock: Clock): Set[String] = {
    getExecutorBlacklistInfo(executorIdToFailureStatus)
      .groupBy{case (id, failureStatus) => failureStatus.host}
      .filter {case (host, executorIdToFailureStatus) =>
        executorIdToFailureStatus.size > maxBlacklistedExecutors}
      .keys.toSet
  }
}

/**
 * This strategy is applied as default to keep the same semantics as original. It's an task
 * related strategy. If an executor failed running "task A", then we think this executor is
 * blacked for "task A". And we think the executor is still healthy for other task. node blacklist
 * is always empty.
 *
 * It was the standard behavior before spark 1.6
 */
private[scheduler] class SingleTaskStrategy(
    val expireTimeInMilliseconds: Long) extends BlacklistStrategy {
  def getExecutorBlacklist(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus],
      atomTask: StageAndPartition,
      clock: Clock): Set[String] = {
    executorIdToFailureStatus.filter{
      case (_, failureStatus) => failureStatus.numFailuresPerTask.keySet.contains(atomTask) &&
        clock.getTimeMillis() - failureStatus.updatedTime < expireTimeInMilliseconds
    }.keys.toSet
  }

  def getNodeBlacklist(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus],
      clock: Clock): Set[String] = Set.empty[String]
}

/**
 * Support getNodeBlacklistForStage. With this strategy, once executor failed running a task, we
 * put all executors on the same node into blacklist, so all tasks on the same stage will not be
 * allocated to that node.
 */
private[scheduler] class AdvancedSingleTaskStrategy(
    expireTimeInMilliseconds: Long) extends SingleTaskStrategy(expireTimeInMilliseconds) {

  override def getNodeBlacklistForStage(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus],
      stageId: Int,
      clock: Clock): Set[String] = {
    executorIdToFailureStatus.filter{
      case (_, failureStatus) =>
        failureStatus.numFailuresPerTask.keySet.map(_.stageId).contains(stageId) &&
        clock.getTimeMillis() - failureStatus.updatedTime < expireTimeInMilliseconds
    }.values.map(_.host).toSet
  }
}

/**
 * Create BlacklistStrategy instance according to SparkConf
 */
private[scheduler] object BlacklistStrategy {
  def apply(sparkConf: SparkConf): BlacklistStrategy = {
    val timeout = sparkConf.getTimeAsMs("spark.scheduler.blacklist.timeout",
        sparkConf.getLong("spark.scheduler.executorTaskBlacklistTime", 0L).toString() + "ms")
    sparkConf.get("spark.scheduler.blacklist.strategy", "singleTask") match {
      case "singleTask" =>
        new SingleTaskStrategy(timeout)
      case "advancedSingleTask" =>
        new AdvancedSingleTaskStrategy(timeout)
      case "executorAndNode" =>
        new ExecutorAndNodeStrategy(
            sparkConf.getInt("spark.scheduler.blacklist.executorAndNode.maxFailedTasks", 3),
            sparkConf.getInt(
                "spark.scheduler.blacklist.executorAndNode.maxBlacklistedExecutors", 3),
            timeout)
      case unsupported =>
        throw new IllegalArgumentException(s"No matching blacklist strategy for: $unsupported")
    }
  }
}
