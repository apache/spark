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
private[scheduler] trait BlacklistStrategy {
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

  /**
   * Return all nodes in blacklist for specified stage. By default it returns the same result as
   * getNodeBlacklist. It could be override in strategy implementation.
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
 * This strategy is applied to keep the same semantics as standard behavior before spark 1.6.
 *
 * If an executor failed running "task A", then we think this executor is blacked for "task A",
 * but at the same time. it is still healthy for other task. Node blacklist is always empty.
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
 * Comparing to SingleTaskStrategy, it supports node blacklist. With this strategy, once more than
 * one executor failed running for specific stage, we put all executors on the same node into
 * blacklist. So all tasks from the same stage will not be allocated to that node.
 */
private[scheduler] class AdvancedSingleTaskStrategy(
    expireTimeInMilliseconds: Long) extends SingleTaskStrategy(expireTimeInMilliseconds) {

  override def getNodeBlacklistForStage(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus],
      stageId: Int,
      clock: Clock): Set[String] = {
    val nodes = executorIdToFailureStatus.filter{
      case (_, failureStatus) =>
        failureStatus.numFailuresPerTask.keySet.map(_.stageId).contains(stageId) &&
        clock.getTimeMillis() - failureStatus.updatedTime < expireTimeInMilliseconds
    }.values.map(_.host)
    getDuplicateElem(nodes, 1)
  }

  override def getNodeBlacklist(
      executorIdToFailureStatus: mutable.HashMap[String, FailureStatus],
      clock: Clock): Set[String] = {
    // resolve a nodes sequence from failure status.
    val nodes = executorIdToFailureStatus.values.map(_.host)
    getDuplicateElem(nodes, 1)
  }

  // A help function to find hosts which have more than "depTimes" executors on it in blacklist
  private def getDuplicateElem(ndoes: Iterable[String], dupTimes: Int): Set[String] = {
    ndoes.groupBy(identity).mapValues(_.size)  // resolve map (nodeName => occurred times)
      .filter(ele => ele._2 > dupTimes)        // return nodes which occurred more than dupTimes.
      .keys.toSet
  }
}

/**
 * Create BlacklistStrategy instance according to SparkConf
 */
private[scheduler] object BlacklistStrategy {
  def apply(sparkConf: SparkConf): BlacklistStrategy = {
    val timeout = sparkConf.getTimeAsMs("spark.scheduler.blacklist.timeout",
        sparkConf.getLong("spark.scheduler.executorTaskBlacklistTime", 0L).toString() + "ms")

    sparkConf.getBoolean("spark.scheduler.blacklist.advancedStrategy", false) match {
      case false => new SingleTaskStrategy(timeout)
      case true => new AdvancedSingleTaskStrategy(timeout)
    }
  }
}
