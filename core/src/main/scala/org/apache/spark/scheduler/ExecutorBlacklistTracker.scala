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

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * ExecutorBlacklistTracker blacklists the executors by tracking the status of running tasks with
 * heuristic algorithm.
 *
 * A executor will be considered bad enough only when:
 * 1. The failure task number on this executor is more than
 *    spark.scheduler.blacklist.executorFaultThreshold.
 * 2. The failure task number on this executor is
 *    spark.scheduler.blacklist.averageBlacklistThreshold more than average failure task number
 *    of this cluster.
 *
 * Also max number of blacklisted executors will not exceed the
 * spark.scheduler.blacklist.maxBlacklistFraction of whole cluster, and blacklisted executors
 * will be forgiven when there is no failure tasks in the
 * spark.scheduler.blacklist.executorFaultTimeoutWindowInMinutes.
 */
private[spark] class ExecutorBlacklistTracker(conf: SparkConf) extends SparkListener {
  import ExecutorBlacklistTracker._

  private val maxBlacklistFraction = conf.getDouble(
    "spark.scheduler.blacklist.maxBlacklistFraction", MAX_BLACKLIST_FRACTION)
  private val avgBlacklistThreshold = conf.getDouble(
    "spark.scheduler.blacklist.averageBlacklistThreshold", AVERAGE_BLACKLIST_THRESHOLD)
  private val executorFaultThreshold = conf.getInt(
    "spark.scheduler.blacklist.executorFaultThreshold", EXECUTOR_FAULT_THRESHOLD)
  private val executorFaultTimeoutWindowInMinutes = conf.getInt(
    "spark.scheduler.blacklist.executorFaultTimeoutWindowInMinutes", EXECUTOR_FAULT_TIMEOUT_WINDOW)

  // Count the number of executors registered
  var numExecutorsRegistered: Int = 0

  // Track the number of failure tasks and time of latest failure to executor id
  val executorIdToTaskFailures = new mutable.HashMap[String, ExecutorFailureStatus]()

  // Clock used to update and exclude the executors which are out of time window.
  private var clock: Clock = new SystemClock()

  // Executor that handles the scheduling task
  private val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    "spark-scheduler-blacklist-expire-timer")

  def start(): Unit = {
    val scheduleTask = new Runnable() {
      override def run(): Unit = {
        Utils.logUncaughtExceptions(expireTimeoutExecutorBlacklist())
      }
    }
    executor.scheduleAtFixedRate(scheduleTask, 0L, 60, TimeUnit.SECONDS)
  }

  def stop(): Unit = {
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  def setClock(newClock: Clock): Unit = {
    clock = newClock
  }

  def getExecutorBlacklist: Set[String] = synchronized {
    executorIdToTaskFailures.filter(_._2.isBlackListed).keys.toSet
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    taskEnd.reason match {
      case _: FetchFailed | _: ExceptionFailure | TaskResultLost |
          _: ExecutorLostFailure | UnknownReason =>
        val failureStatus = executorIdToTaskFailures.getOrElseUpdate(taskEnd.taskInfo.executorId,
          new ExecutorFailureStatus)
        failureStatus.numFailures += 1
        failureStatus.updatedTime = clock.getTimeMillis()

        // Update the executor blacklist
        updateExecutorBlacklist()
      case _ => Unit
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    numExecutorsRegistered += 1
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved
      ): Unit = synchronized {
    numExecutorsRegistered -= 1
    executorIdToTaskFailures -= executorRemoved.executorId
  }

  private def updateExecutorBlacklist(): Unit = {
    // Filter out the executor Ids where task failure number is larger than
    // executorFaultThreshold and not blacklisted
    val failedExecutors = executorIdToTaskFailures.filter { case(_, e) =>
      e.numFailures >= executorFaultThreshold && !e.isBlackListed
    }

    val blacklistedExecutorNum = executorIdToTaskFailures.filter(_._2.isBlackListed).size

    if (failedExecutors.nonEmpty) {
      val avgNumFailed = executorIdToTaskFailures.values.map(_.numFailures).sum.toDouble /
        numExecutorsRegistered
      for ((executorId, failureStatus) <- failedExecutors) {
        // If the number of failure task is more than average blacklist threshold of average
        // failed number and current executor blacklist is less than the max fraction of number
        // executors
        if ((failureStatus.numFailures.toDouble > avgNumFailed * (1 + avgBlacklistThreshold)) &&
          (blacklistedExecutorNum.toDouble < numExecutorsRegistered * maxBlacklistFraction)) {
          failureStatus.isBlackListed = true
        }
      }
    }
  }

  private def expireTimeoutExecutorBlacklist(): Unit = synchronized {
    val now = clock.getTimeMillis()

    executorIdToTaskFailures.foreach { case (id, failureStatus) =>
      if ((now - failureStatus.updatedTime) > executorFaultTimeoutWindowInMinutes * 60 * 1000
        && failureStatus.isBlackListed) {
        failureStatus.isBlackListed = false
      }
    }
  }
}

private[spark] object ExecutorBlacklistTracker {
  // The maximum fraction (range [0.0-1.0]) of executors in cluster allowed to be added to the
  // blacklist via heuristics.  By default, no more than 50% of the executors can be
  // heuristically blacklisted.
  val MAX_BLACKLIST_FRACTION = 0.5

  // A executor is blacklisted only if number of faults is more than X% above the average number
  // of faults (averaged across all executor in cluster).  X is the blacklist threshold here; 0.3
  // would correspond to 130% of the average, for example.
  val AVERAGE_BLACKLIST_THRESHOLD = 0.5

  // Fault threshold (number occurring within EXECUTOR_FAULT_TIMEOUT_WINDOW)
  // to consider a executor bad enough to blacklist heuristically.
  val EXECUTOR_FAULT_THRESHOLD = 4

  // Width of overall fault-tracking sliding window (in minutes), that was used to forgive a
  // single fault if no others occurred in the interval.
  val EXECUTOR_FAULT_TIMEOUT_WINDOW = 180

  final class ExecutorFailureStatus {
    var numFailures: Int = 0
    var updatedTime: Long = 0L
    var isBlackListed: Boolean = false
  }
}
