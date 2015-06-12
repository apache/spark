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

import java.util.concurrent.{TimeUnit, ConcurrentLinkedQueue}

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.util.{SystemClock, ThreadUtils, Utils}

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
  private var numExecutorsRegistered: Int = 0

  // Track the number of failure tasks to executor id
  private val executorIdToTaskFailures = new mutable.HashMap[String, Int]()

  // Track the executor id to host mapping relation
  private val executorIdToHosts = new mutable.HashMap[String, String]()

  // Maintain the executor blacklist
  private val executorBlacklist = new ConcurrentLinkedQueue[(String, Long)]()

  // Clock used to update and exclude the executors which are out of time window.
  private val clock = new SystemClock()

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

  def getExecutorBlacklist: Set[String] = {
    val executors = new Array[(String, Long)](executorBlacklist.size())
    executorBlacklist.toArray(executors).map(_._1).toSet
  }

  def getHostBlacklist: Set[String] = {
    val executorBlacklist = getExecutorBlacklist
    executorBlacklist.map(executorIdToHosts.get(_)).flatMap(x => x).toSet
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    taskEnd.reason match {
      case _: FetchFailed | _: ExceptionFailure | TaskResultLost |
          _: ExecutorLostFailure | UnknownReason =>
        val numFailures = executorIdToTaskFailures.getOrElseUpdate(
          taskEnd.taskInfo.executorId, 0) + 1
        executorIdToTaskFailures.put(taskEnd.taskInfo.executorId, numFailures)
        // Update the executor blacklist
        updateExecutorBlacklist()
      case _ => Unit
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    numExecutorsRegistered += 1
    executorIdToHosts(executorAdded.executorId) = executorAdded.executorInfo.executorHost
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    numExecutorsRegistered -= 1
    executorIdToHosts -= executorRemoved.executorId
  }

  private def updateExecutorBlacklist(): Unit = {
    // Filter out the executor Ids where task failure number is larger than executorFaultThreshold
    val failedExecutors = executorIdToTaskFailures.filter(_._2 >= executorFaultThreshold)
    if (!failedExecutors.isEmpty) {
      val avgNumFailed = executorIdToTaskFailures.values.sum.toDouble / executorBlacklist.size
      for ((executorId, numFailed) <- failedExecutors) {
        // If the number of failure task is more than average blacklist threshold of average
        // failed number and current executor blacklist is less than the max fraction of number
        // executors
        if ((numFailed.toDouble > avgNumFailed * (1 + avgBlacklistThreshold)) &&
          (executorBlacklist.size.toDouble < numExecutorsRegistered * maxBlacklistFraction)) {
          executorBlacklist.add((executorId, clock.getTimeMillis()))
          executorIdToTaskFailures -= executorId
        }
      }
    }
  }

  private def expireTimeoutExecutorBlacklist(): Unit = {
    val now = clock.getTimeMillis()
    var loop = true

    while (loop) {
      Option(executorBlacklist.peek()) match {
        case Some((executorId, addedTime)) =>
          if ((now - addedTime) > executorFaultTimeoutWindowInMinutes * 60 * 1000) {
            executorBlacklist.poll()
          } else {
            loop = false
          }
        case None => loop = false
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
  // single fault if no others occurred in the interval.)
  val EXECUTOR_FAULT_TIMEOUT_WINDOW = 180
}