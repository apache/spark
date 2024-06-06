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

package org.apache.spark.deploy

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Streaming.STREAMING_DYN_ALLOCATION_MAX_EXECUTORS
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * ExecutorFailureTracker is responsible for tracking executor failures both for each host
 * separately and for all hosts altogether.
 */
private[spark] class ExecutorFailureTracker(
  sparkConf: SparkConf,
  val clock: Clock = new SystemClock) extends Logging {

  private val executorFailuresValidityInterval =
    sparkConf.get(EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS).getOrElse(-1L)

  // Queue to store the timestamp of failed executors for each host
  private val failedExecutorsTimeStampsPerHost = mutable.Map[String, mutable.Queue[Long]]()

  private val failedExecutorsTimeStamps = new mutable.Queue[Long]()

  private def updateAndCountFailures(failedExecutorsWithTimeStamps: mutable.Queue[Long]): Int = {
    val endTime = clock.getTimeMillis()
    while (executorFailuresValidityInterval > 0 &&
      failedExecutorsWithTimeStamps.nonEmpty &&
      failedExecutorsWithTimeStamps.head < endTime - executorFailuresValidityInterval) {
      failedExecutorsWithTimeStamps.dequeue()
    }
    failedExecutorsWithTimeStamps.size
  }

  def numFailedExecutors: Int = synchronized {
    updateAndCountFailures(failedExecutorsTimeStamps)
  }

  def registerFailureOnHost(hostname: String): Unit = synchronized {
    val timeMillis = clock.getTimeMillis()
    failedExecutorsTimeStamps.enqueue(timeMillis)
    val failedExecutorsOnHost =
      failedExecutorsTimeStampsPerHost.getOrElse(hostname, {
        val failureOnHost = mutable.Queue[Long]()
        failedExecutorsTimeStampsPerHost.put(hostname, failureOnHost)
        failureOnHost
      })
    failedExecutorsOnHost.enqueue(timeMillis)
  }

  def registerExecutorFailure(): Unit = synchronized {
    val timeMillis = clock.getTimeMillis()
    failedExecutorsTimeStamps.enqueue(timeMillis)
  }

  def numFailuresOnHost(hostname: String): Int = {
    failedExecutorsTimeStampsPerHost.get(hostname).map { failedExecutorsOnHost =>
      updateAndCountFailures(failedExecutorsOnHost)
    }.getOrElse(0)
  }
}

object ExecutorFailureTracker {

  // Default to twice the number of executors (twice the maximum number of executors if dynamic
  // allocation is enabled), with a minimum of 3.
  def maxNumExecutorFailures(sparkConf: SparkConf): Int = {
    // By default, effectiveNumExecutors is Int.MaxValue if dynamic allocation is enabled. We need
    // avoid the integer overflow here.
    def defaultMaxNumExecutorFailures: Int = {
      val effectiveNumExecutors =
        if (Utils.isStreamingDynamicAllocationEnabled(sparkConf)) {
          sparkConf.get(STREAMING_DYN_ALLOCATION_MAX_EXECUTORS)
        } else if (Utils.isDynamicAllocationEnabled(sparkConf)) {
          sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS)
        } else {
          sparkConf.get(EXECUTOR_INSTANCES).getOrElse(0)
        }
      math.max(3,
        if (effectiveNumExecutors > Int.MaxValue / 2) Int.MaxValue else 2 * effectiveNumExecutors)
    }

    sparkConf.get(MAX_EXECUTOR_FAILURES).getOrElse(defaultMaxNumExecutorFailures)
  }
}
